// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

import {SafeTransferLib} from "./solady/utils/SafeTransferLib.sol";

interface IWETHMinimal {
    function deposit() external payable;
}

interface IERC2612Minimal {
    function permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s)
        external;
}

interface IPermit2SignatureTransfer {
    struct TokenPermissions {
        address token;
        uint256 amount;
    }

    struct PermitTransferFrom {
        TokenPermissions permitted;
        uint256 nonce;
        uint256 deadline;
    }

    struct SignatureTransferDetails {
        address to;
        uint256 requestedAmount;
    }

    function permitTransferFrom(
        PermitTransferFrom calldata permit,
        SignatureTransferDetails calldata transferDetails,
        address owner,
        bytes calldata signature
    ) external;
}

interface IBalancerV2VaultMinimal {
    enum SwapKind {
        GIVEN_IN,
        GIVEN_OUT
    }

    struct SingleSwap {
        bytes32 poolId;
        SwapKind kind;
        address assetIn;
        address assetOut;
        uint256 amount;
        bytes userData;
    }

    struct FundManagement {
        address sender;
        bool fromInternalBalance;
        address payable recipient;
        bool toInternalBalance;
    }

    function swap(SingleSwap memory singleSwap, FundManagement memory funds, uint256 limit, uint256 deadline)
        external
        returns (uint256 amountCalculated);
}

/// @notice EXPERIMENTAL demonstration router for exercising routes produced by
/// evm-amm-search.
/// @dev ABSOLUTELY NOT INTENDED FOR PUBLIC OR PRODUCTION USE. This contract is
/// unaudited, accepts caller-supplied route endpoints, and is under active
/// development. Its interface and packed route format may change without notice.
contract ExperimentalExecutorRouter {
    using SafeTransferLib for address;

    string public constant SAFETY_WARNING = "EXPERIMENTAL: NOT INTENDED FOR PUBLIC OR PRODUCTION USE";

    uint8 private constant PROTOCOL_UNISWAP_V2 = 0;
    uint8 private constant PROTOCOL_UNISWAP_V3 = 1;
    uint8 private constant PROTOCOL_PANCAKE_V3 = 2;
    uint8 private constant PROTOCOL_SLIPSTREAM = 3;
    uint8 private constant PROTOCOL_SOLIDLY_V2 = 4;
    uint8 private constant PROTOCOL_BALANCER_V2 = 5;
    uint8 private constant PROTOCOL_CURVE_STABLE = 6;
    uint8 private constant PROTOCOL_CURVE_CRYPTO = 7;
    uint8 private constant PROTOCOL_CURVE_CRYPTO_NG = 8;
    uint256 private constant HOP_COMMON_SIZE = 61;

    uint160 private constant MIN_SQRT_RATIO = 4295128739 + 1;
    uint160 private constant MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342 - 1;

    address public immutable WETH;
    address public immutable PERMIT2;
    uint256 private unlocked = 1;
    address private callbackPool;
    address private callbackToken;
    uint256 private callbackAmount;

    struct ExactInputParams {
        address tokenIn;
        address tokenOut;
        uint256 amountIn;
        uint256 minAmountOut;
        address recipient;
        uint256 deadline;
        bytes route;
    }

    struct ERC2612Permit {
        uint8 v;
        bytes32 r;
        bytes32 s;
    }

    struct Permit2SignatureTransfer {
        uint256 nonce;
        uint256 deadline;
        bytes signature;
    }

    error InvalidPackedRoute();
    error InvalidRouteToken(address expected, address actual);
    error UnsupportedProtocol(uint8 protocol);
    error InsufficientOutput(uint256 minimum, uint256 actual);
    error ZeroRecipient();
    error Expired(uint256 deadline, uint256 timestamp);
    error Reentrancy();
    error InvalidNativeInput(address expectedToken, address actualToken, uint256 expectedAmount, uint256 actualAmount);
    error InvalidCallback(address caller, address expected);
    error InvalidCallbackPayment(uint256 requested, uint256 remaining);
    error InvalidCallbackDeltas(int256 amount0Delta, int256 amount1Delta);
    error ZeroMinAmountOut();
    error ZeroDeploymentDependency();
    error InvalidFeeBps(uint256 feeBps);
    error InvalidTokenPair(address token);
    error InvalidInputAmount(uint256 expected, uint256 received);
    error InvalidRecipientOutput(uint256 expected, uint256 received);

    modifier nonReentrant() {
        if (unlocked != 1) revert Reentrancy();
        unlocked = 2;
        _;
        unlocked = 1;
    }
    error IncompleteHopConsumption(address token, uint256 expected, uint256 consumed);
    error UnsafeEndBalance(address token, uint256 expected, uint256 actual);

    constructor(address weth, address permit2) {
        if (weth == address(0) || permit2 == address(0)) revert ZeroDeploymentDependency();
        WETH = weth;
        PERMIT2 = permit2;
    }

    function executeExactInput(ExactInputParams calldata params) external nonReentrant returns (uint256 amountOut) {
        _validate(params);
        uint256 inputBalanceBefore = _balanceOf(params.tokenIn, address(this));
        uint256 finalBalanceBefore = _balanceOf(params.tokenOut, address(this));
        params.tokenIn.safeTransferFrom(msg.sender, address(this), params.amountIn);
        _requireExactFunding(params.tokenIn, inputBalanceBefore, params.amountIn);
        amountOut = _execute(params, inputBalanceBefore, finalBalanceBefore);
    }

    function executeExactInputNative(ExactInputParams calldata params)
        external
        payable
        nonReentrant
        returns (uint256 amountOut)
    {
        _validate(params);
        if (params.tokenIn != WETH || params.amountIn != msg.value) {
            revert InvalidNativeInput(WETH, params.tokenIn, params.amountIn, msg.value);
        }
        uint256 ethBalanceBefore = address(this).balance - msg.value;
        uint256 inputBalanceBefore = _balanceOf(params.tokenIn, address(this));
        uint256 finalBalanceBefore = _balanceOf(params.tokenOut, address(this));
        IWETHMinimal(WETH).deposit{value: msg.value}();
        _requireExactFunding(params.tokenIn, inputBalanceBefore, params.amountIn);
        amountOut = _execute(params, inputBalanceBefore, finalBalanceBefore);
        if (address(this).balance != ethBalanceBefore) {
            revert UnsafeEndBalance(address(0), ethBalanceBefore, address(this).balance);
        }
    }

    function executeExactInputWithPermit(ExactInputParams calldata params, ERC2612Permit calldata permit)
        external
        nonReentrant
        returns (uint256 amountOut)
    {
        _validate(params);
        IERC2612Minimal(params.tokenIn)
            .permit(msg.sender, address(this), params.amountIn, params.deadline, permit.v, permit.r, permit.s);
        uint256 inputBalanceBefore = _balanceOf(params.tokenIn, address(this));
        uint256 finalBalanceBefore = _balanceOf(params.tokenOut, address(this));
        params.tokenIn.safeTransferFrom(msg.sender, address(this), params.amountIn);
        _requireExactFunding(params.tokenIn, inputBalanceBefore, params.amountIn);
        amountOut = _execute(params, inputBalanceBefore, finalBalanceBefore);
    }

    function executeExactInputWithPermit2(ExactInputParams calldata params, Permit2SignatureTransfer calldata permit)
        external
        nonReentrant
        returns (uint256 amountOut)
    {
        _validate(params);
        uint256 inputBalanceBefore = _balanceOf(params.tokenIn, address(this));
        uint256 finalBalanceBefore = _balanceOf(params.tokenOut, address(this));
        IPermit2SignatureTransfer(PERMIT2)
            .permitTransferFrom(
                IPermit2SignatureTransfer.PermitTransferFrom({
                permitted: IPermit2SignatureTransfer.TokenPermissions({token: params.tokenIn, amount: params.amountIn}),
                nonce: permit.nonce,
                deadline: permit.deadline
            }),
                IPermit2SignatureTransfer.SignatureTransferDetails({
                to: address(this), requestedAmount: params.amountIn
            }),
                msg.sender,
                permit.signature
            );
        _requireExactFunding(params.tokenIn, inputBalanceBefore, params.amountIn);
        amountOut = _execute(params, inputBalanceBefore, finalBalanceBefore);
    }

    function _validate(ExactInputParams calldata params) private view {
        if (params.recipient == address(0)) revert ZeroRecipient();
        if (params.tokenIn == params.tokenOut) revert InvalidTokenPair(params.tokenIn);
        if (params.minAmountOut == 0) revert ZeroMinAmountOut();
        // Deadlines are intentionally expressed in EVM time, like the supported AMM entry points.
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > params.deadline) revert Expired(params.deadline, block.timestamp);
    }

    function _execute(ExactInputParams calldata params, uint256 startingInputBalance, uint256 finalBalanceBefore)
        private
        returns (uint256 amountOut)
    {
        bytes calldata route = params.route;
        _requireRoute(route, 0, 20 + HOP_COMMON_SIZE);
        address encodedFinalToken = _readAddress(route, 0);
        if (encodedFinalToken != params.tokenOut) {
            revert InvalidRouteToken(params.tokenOut, encodedFinalToken);
        }

        address currentToken = params.tokenIn;
        uint256 currentAmount = params.amountIn;
        uint256 offset = 20;
        while (offset < route.length) {
            _requireRoute(route, offset, HOP_COMMON_SIZE);
            uint8 protocol = _readUint8(route, offset);
            address endpoint = _readAddress(route, offset + 1);
            address tokenIn = _readAddress(route, offset + 21);
            address tokenOut = _readAddress(route, offset + 41);
            uint256 nextOffset = offset + HOP_COMMON_SIZE;
            if (tokenIn != currentToken) revert InvalidRouteToken(currentToken, tokenIn);

            uint256 hopInputBalanceBefore = _balanceOf(tokenIn, address(this));
            uint256 outputBalanceBefore = _balanceOf(tokenOut, address(this));
            if (protocol == PROTOCOL_UNISWAP_V2) {
                _requireRoute(route, nextOffset, 2);
                uint256 feeBps = _readUint16(route, nextOffset);
                _swapV2(endpoint, tokenIn, tokenOut, currentAmount, feeBps);
                nextOffset += 2;
            } else if (
                protocol == PROTOCOL_UNISWAP_V3 || protocol == PROTOCOL_PANCAKE_V3 || protocol == PROTOCOL_SLIPSTREAM
            ) {
                _swapV3Family(endpoint, tokenIn, tokenOut, currentAmount);
            } else if (protocol == PROTOCOL_SOLIDLY_V2) {
                _swapSolidlyV2(endpoint, tokenIn, tokenOut, currentAmount);
            } else if (protocol == PROTOCOL_BALANCER_V2) {
                _requireRoute(route, nextOffset, 32);
                bytes32 poolId = _readBytes32(route, nextOffset);
                _swapBalancerV2(endpoint, poolId, tokenIn, tokenOut, currentAmount);
                nextOffset += 32;
            } else if (
                protocol == PROTOCOL_CURVE_STABLE || protocol == PROTOCOL_CURVE_CRYPTO
                    || protocol == PROTOCOL_CURVE_CRYPTO_NG
            ) {
                _requireRoute(route, nextOffset, 2);
                uint256 i = _readUint8(route, nextOffset);
                uint256 j = _readUint8(route, nextOffset + 1);
                _swapCurve(endpoint, tokenIn, currentAmount, i, j, protocol);
                nextOffset += 2;
            } else {
                revert UnsupportedProtocol(protocol);
            }
            uint256 hopInputBalanceAfter = _balanceOf(tokenIn, address(this));
            uint256 consumed = hopInputBalanceBefore - hopInputBalanceAfter;
            if (consumed != currentAmount) {
                revert IncompleteHopConsumption(tokenIn, currentAmount, consumed);
            }
            currentAmount = _balanceOf(tokenOut, address(this)) - outputBalanceBefore;
            currentToken = tokenOut;
            offset = nextOffset;
        }

        if (currentToken != params.tokenOut) revert InvalidRouteToken(params.tokenOut, currentToken);
        amountOut = currentAmount;
        if (amountOut < params.minAmountOut) revert InsufficientOutput(params.minAmountOut, amountOut);
        uint256 recipientBalanceBefore = _balanceOf(params.tokenOut, params.recipient);
        params.tokenOut.safeTransfer(params.recipient, amountOut);
        uint256 recipientBalanceAfter = _balanceOf(params.tokenOut, params.recipient);
        uint256 recipientAmount =
            recipientBalanceAfter >= recipientBalanceBefore ? recipientBalanceAfter - recipientBalanceBefore : 0;
        if (recipientAmount != amountOut) revert InvalidRecipientOutput(amountOut, recipientAmount);
        uint256 finalBalanceAfter = _balanceOf(params.tokenOut, address(this));
        if (finalBalanceAfter != finalBalanceBefore) {
            revert UnsafeEndBalance(params.tokenOut, finalBalanceBefore, finalBalanceAfter);
        }
        uint256 endingInputBalance = _balanceOf(params.tokenIn, address(this));
        if (endingInputBalance != startingInputBalance) {
            revert UnsafeEndBalance(params.tokenIn, startingInputBalance, endingInputBalance);
        }
    }

    function _requireExactFunding(address token, uint256 balanceBefore, uint256 expected) private view {
        uint256 balanceAfter = _balanceOf(token, address(this));
        uint256 received = balanceAfter >= balanceBefore ? balanceAfter - balanceBefore : 0;
        if (received != expected) revert InvalidInputAmount(expected, received);
    }

    function _swapV2(address pair, address tokenIn, address tokenOut, uint256 amountIn, uint256 feeBps) private {
        if (feeBps >= 10_000) revert InvalidFeeBps(feeBps);
        bool zeroForOne = tokenIn < tokenOut;
        (uint256 reserve0, uint256 reserve1) = _getReserves(pair);
        (uint256 reserveIn, uint256 reserveOut) = zeroForOne ? (reserve0, reserve1) : (reserve1, reserve0);
        uint256 amountOut = _getAmountOut(amountIn, reserveIn, reserveOut, feeBps);
        tokenIn.safeTransfer(pair, amountIn);
        _callPairSwap(pair, zeroForOne ? 0 : amountOut, zeroForOne ? amountOut : 0);
    }

    function _swapV3Family(address pool, address tokenIn, address tokenOut, uint256 amountIn) private {
        bool zeroForOne = tokenIn < tokenOut;
        callbackPool = pool;
        callbackToken = tokenIn;
        callbackAmount = amountIn;
        _callV3Swap(pool, zeroForOne, amountIn);
        callbackPool = address(0);
        callbackToken = address(0);
        callbackAmount = 0;
    }

    function _swapSolidlyV2(address pool, address tokenIn, address tokenOut, uint256 amountIn) private {
        bool zeroForOne = tokenIn < tokenOut;
        uint256 amountOut = _solidlyAmountOut(pool, amountIn, tokenIn);
        tokenIn.safeTransfer(pool, amountIn);
        _callPairSwap(pool, zeroForOne ? 0 : amountOut, zeroForOne ? amountOut : 0);
    }

    function _swapBalancerV2(address vault, bytes32 poolId, address tokenIn, address tokenOut, uint256 amountIn)
        private
    {
        tokenIn.safeApproveWithRetry(vault, amountIn);
        IBalancerV2VaultMinimal(vault)
            .swap(
                IBalancerV2VaultMinimal.SingleSwap({
                poolId: poolId,
                kind: IBalancerV2VaultMinimal.SwapKind.GIVEN_IN,
                assetIn: tokenIn,
                assetOut: tokenOut,
                amount: amountIn,
                userData: ""
            }),
                IBalancerV2VaultMinimal.FundManagement({
                sender: address(this),
                fromInternalBalance: false,
                recipient: payable(address(this)),
                toInternalBalance: false
            }),
                0,
                block.timestamp
            );
        tokenIn.safeApprove(vault, 0);
    }

    function _swapCurve(address pool, address tokenIn, uint256 amountIn, uint256 i, uint256 j, uint8 protocol) private {
        tokenIn.safeApproveWithRetry(pool, amountIn);
        if (protocol == PROTOCOL_CURVE_STABLE) {
            _callCurve4(pool, 0x3df02124, i, j, amountIn);
        } else if (protocol == PROTOCOL_CURVE_CRYPTO) {
            _callCurve4(pool, 0x5b41b908, i, j, amountIn);
        } else {
            _callCurve5(pool, 0x394747c5, i, j, amountIn);
        }
        tokenIn.safeApprove(pool, 0);
    }

    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata) external {
        _v3PayCallback(amount0Delta, amount1Delta);
    }

    function pancakeV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata) external {
        _v3PayCallback(amount0Delta, amount1Delta);
    }

    function _v3PayCallback(int256 amount0Delta, int256 amount1Delta) private {
        address expectedPool = callbackPool;
        if (msg.sender != expectedPool || expectedPool == address(0)) {
            revert InvalidCallback(msg.sender, expectedPool);
        }
        bool amount0Positive = amount0Delta > 0;
        bool amount1Positive = amount1Delta > 0;
        if (amount0Positive == amount1Positive) revert InvalidCallbackDeltas(amount0Delta, amount1Delta);
        uint256 amountToPay;
        assembly ("memory-safe") {
            amountToPay := xor(amount0Delta, mul(xor(amount0Delta, amount1Delta), amount1Positive))
        }
        uint256 remaining = callbackAmount;
        if (amountToPay == 0 || amountToPay > remaining) {
            revert InvalidCallbackPayment(amountToPay, remaining);
        }
        callbackAmount = remaining - amountToPay;
        callbackToken.safeTransfer(msg.sender, amountToPay);
    }

    function _balanceOf(address token, address owner) private view returns (uint256 amount) {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x70a08231))
            mstore(add(ptr, 0x04), owner)
            if iszero(staticcall(gas(), token, ptr, 0x24, ptr, 0x20)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
            amount := mload(ptr)
        }
    }

    function _getReserves(address pair) private view returns (uint256 reserve0, uint256 reserve1) {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x0902f1ac))
            if iszero(staticcall(gas(), pair, ptr, 0x04, ptr, 0x60)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
            reserve0 := mload(ptr)
            reserve1 := mload(add(ptr, 0x20))
        }
    }

    function _solidlyAmountOut(address pool, uint256 amountIn, address tokenIn)
        private
        view
        returns (uint256 amountOut)
    {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0xf140a35a))
            mstore(add(ptr, 0x04), amountIn)
            mstore(add(ptr, 0x24), tokenIn)
            if iszero(staticcall(gas(), pool, ptr, 0x44, ptr, 0x20)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
            amountOut := mload(ptr)
        }
    }

    function _callPairSwap(address pair, uint256 amount0Out, uint256 amount1Out) private {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x022c0d9f))
            mstore(add(ptr, 0x04), amount0Out)
            mstore(add(ptr, 0x24), amount1Out)
            mstore(add(ptr, 0x44), address())
            mstore(add(ptr, 0x64), 0x80)
            mstore(add(ptr, 0x84), 0)
            if iszero(call(gas(), pair, 0, ptr, 0xa4, 0, 0)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
        }
    }

    function _callV3Swap(address pool, bool zeroForOne, uint256 amountIn) private {
        uint160 sqrtPriceLimitX96 = zeroForOne ? MIN_SQRT_RATIO : MAX_SQRT_RATIO;
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x128acb08))
            mstore(add(ptr, 0x04), address())
            mstore(add(ptr, 0x24), zeroForOne)
            mstore(add(ptr, 0x44), amountIn)
            mstore(add(ptr, 0x64), sqrtPriceLimitX96)
            mstore(add(ptr, 0x84), 0xa0)
            mstore(add(ptr, 0xa4), 0)
            if iszero(call(gas(), pool, 0, ptr, 0xc4, 0, 0x40)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
        }
    }

    function _callCurve4(address pool, uint256 selector, uint256 i, uint256 j, uint256 amountIn) private {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, selector))
            mstore(add(ptr, 0x04), i)
            mstore(add(ptr, 0x24), j)
            mstore(add(ptr, 0x44), amountIn)
            mstore(add(ptr, 0x64), 0)
            if iszero(call(gas(), pool, 0, ptr, 0x84, 0, 0x20)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
        }
    }

    function _callCurve5(address pool, uint256 selector, uint256 i, uint256 j, uint256 amountIn) private {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, selector))
            mstore(add(ptr, 0x04), i)
            mstore(add(ptr, 0x24), j)
            mstore(add(ptr, 0x44), amountIn)
            mstore(add(ptr, 0x64), 0)
            mstore(add(ptr, 0x84), 0)
            if iszero(call(gas(), pool, 0, ptr, 0xa4, 0, 0x20)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
        }
    }

    function _getAmountOut(uint256 amountIn, uint256 reserveIn, uint256 reserveOut, uint256 feeBps)
        private
        pure
        returns (uint256)
    {
        uint256 amountInWithFee = amountIn * (10_000 - feeBps);
        return (amountInWithFee * reserveOut) / (reserveIn * 10_000 + amountInWithFee);
    }

    function _requireRoute(bytes calldata data, uint256 offset, uint256 length) private pure {
        if (offset > data.length || data.length - offset < length) revert InvalidPackedRoute();
    }

    function _readUint8(bytes calldata data, uint256 offset) private pure returns (uint8 value) {
        assembly ("memory-safe") {
            value := byte(0, calldataload(add(data.offset, offset)))
        }
    }

    function _readUint16(bytes calldata data, uint256 offset) private pure returns (uint16 value) {
        assembly ("memory-safe") {
            value := shr(240, calldataload(add(data.offset, offset)))
        }
    }

    function _readAddress(bytes calldata data, uint256 offset) private pure returns (address value) {
        assembly ("memory-safe") {
            value := shr(96, calldataload(add(data.offset, offset)))
        }
    }

    function _readBytes32(bytes calldata data, uint256 offset) private pure returns (bytes32 value) {
        assembly ("memory-safe") {
            value := calldataload(add(data.offset, offset))
        }
    }
}
