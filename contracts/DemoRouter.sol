// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {SafeTransferLib} from "./solady/utils/SafeTransferLib.sol";

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

/// @notice Demonstration-only router for local route simulation and gas estimates.
/// @dev Do not deploy or use in production. This contract intentionally omits
/// production router protections such as slippage checks, access control,
/// route validation beyond calldata shape, deadline handling, and robust
/// protocol-specific edge cases.
contract DemoRouter {
    using SafeTransferLib for address;

    uint8 private constant PROTOCOL_UNISWAP_V2 = 0;
    uint8 private constant PROTOCOL_UNISWAP_V3 = 1;
    uint8 private constant PROTOCOL_PANCAKE_V3 = 2;
    uint8 private constant PROTOCOL_SLIPSTREAM = 3;
    uint8 private constant PROTOCOL_SOLIDLY_V2 = 4;
    uint8 private constant PROTOCOL_BALANCER_V2 = 5;
    uint8 private constant PROTOCOL_CURVE_STABLE = 6;
    uint8 private constant PROTOCOL_CURVE_CRYPTO = 7;
    uint8 private constant PROTOCOL_CURVE_CRYPTO_NG = 8;
    uint8 private constant PROTOCOL_GENERIC_CALL = 255;

    uint256 private constant HOP_COMMON_SIZE = 61;
    uint256 private constant UINT32_NONE = type(uint32).max;

    uint160 private constant MIN_SQRT_RATIO = 4295128739 + 1;
    uint160 private constant MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342 - 1;

    error InvalidPackedRoute();
    error UnsupportedProtocol(uint8 protocol);
    error GenericCallFailed(address target, bytes reason);
    error InvalidAmountPatch(uint256 offset, uint256 length);
    error InvalidCallback(address caller, address expected);
    error InvalidV3Swap();
    error ApproveFailed(address token, address spender);

    receive() external payable {}

    /// @dev Selector mined for `execute_p43bff2e1(uint256,bytes) == 0x00000000`.
    /// Packed route: final token address (20 bytes), followed by packed hop records.
    function execute_p43bff2e1(uint256 amountIn, bytes calldata route) external returns (uint256 amountOut) {
        amountOut = _execute(amountIn, route, address(0));
    }

    function executeFrom(address sender, address tokenIn, uint256 amountIn, bytes calldata route)
        external
        returns (uint256 amountOut)
    {
        tokenIn.safeTransferFrom(sender, address(this), amountIn);
        amountOut = _execute(amountIn, route, sender);
    }

    function _execute(uint256 amountIn, bytes calldata route, address recipient) private returns (uint256 amountOut) {
        _requireRoute(route, 0, 20);
        address finalToken = _readAddress(route, 0);
        uint256 beforeOut = _balanceOf(finalToken, address(this));
        uint256 currentAmount = amountIn;
        uint256 offset = 20;

        while (offset < route.length) {
            (currentAmount, offset) = _executeHop(route, offset, currentAmount);
        }

        uint256 afterOut = _balanceOf(finalToken, address(this));
        amountOut = afterOut - beforeOut;
        if (recipient != address(0)) {
            finalToken.safeTransfer(recipient, amountOut);
        }
    }

    function _executeHop(bytes calldata route, uint256 offset, uint256 amountIn)
        private
        returns (uint256 amountOut, uint256 nextOffset)
    {
        _requireRoute(route, offset, HOP_COMMON_SIZE);
        uint8 protocol = _readUint8(route, offset);
        address endpoint = _readAddress(route, offset + 1);
        address tokenIn = _readAddress(route, offset + 21);
        address tokenOut = _readAddress(route, offset + 41);
        nextOffset = offset + HOP_COMMON_SIZE;

        if (protocol == PROTOCOL_UNISWAP_V2) {
            amountOut = _swapV2(endpoint, tokenIn, tokenOut, amountIn);
        } else if (
            protocol == PROTOCOL_UNISWAP_V3 || protocol == PROTOCOL_PANCAKE_V3 || protocol == PROTOCOL_SLIPSTREAM
        ) {
            amountOut = _swapV3Family(endpoint, tokenIn, tokenOut, amountIn);
        } else if (protocol == PROTOCOL_SOLIDLY_V2) {
            amountOut = _swapSolidlyV2(endpoint, tokenIn, tokenOut, amountIn);
        } else if (protocol == PROTOCOL_BALANCER_V2) {
            _requireRoute(route, nextOffset, 32);
            bytes32 poolId = _readBytes32(route, nextOffset);
            nextOffset += 32;
            amountOut = _swapBalancerV2(endpoint, poolId, tokenIn, tokenOut, amountIn);
        } else if (
            protocol == PROTOCOL_CURVE_STABLE || protocol == PROTOCOL_CURVE_CRYPTO || protocol == PROTOCOL_CURVE_CRYPTO_NG
        ) {
            _requireRoute(route, nextOffset, 2);
            uint256 i = _readUint8(route, nextOffset);
            uint256 j = _readUint8(route, nextOffset + 1);
            nextOffset += 2;
            amountOut = _swapCurve(endpoint, tokenIn, tokenOut, amountIn, i, j, protocol);
        } else if (protocol == PROTOCOL_GENERIC_CALL) {
            (amountOut, nextOffset) = _genericCall(route, nextOffset, endpoint, tokenIn, tokenOut, amountIn);
        } else {
            revert UnsupportedProtocol(protocol);
        }
    }

    function _swapV2(address pair, address tokenIn, address tokenOut, uint256 amountIn)
        private
        returns (uint256 amountOut)
    {
        bool zeroForOne = tokenIn < tokenOut;
        (uint256 reserve0, uint256 reserve1) = _getReserves(pair);
        (uint256 reserveIn, uint256 reserveOut) = zeroForOne ? (reserve0, reserve1) : (reserve1, reserve0);
        amountOut = _getAmountOut(amountIn, reserveIn, reserveOut);
        tokenIn.safeTransfer(pair, amountIn);
        _callPairSwap(pair, zeroForOne ? 0 : amountOut, zeroForOne ? amountOut : 0);
    }

    function _swapSolidlyV2(address pool, address tokenIn, address tokenOut, uint256 amountIn)
        private
        returns (uint256 amountOut)
    {
        bool zeroForOne = tokenIn < tokenOut;
        amountOut = _solidlyAmountOut(pool, amountIn, tokenIn);
        tokenIn.safeTransfer(pool, amountIn);
        _callPairSwap(pool, zeroForOne ? 0 : amountOut, zeroForOne ? amountOut : 0);
    }

    function _swapV3Family(address pool, address tokenIn, address tokenOut, uint256 amountIn)
        private
        returns (uint256 amountOut)
    {
        bool zeroForOne = tokenIn < tokenOut;
        (int256 amount0, int256 amount1) = _callV3Swap(pool, tokenIn, zeroForOne, amountIn);
        int256 amountOutDelta = zeroForOne ? amount1 : amount0;
        if (amountOutDelta > 0) revert InvalidV3Swap();
        amountOut = uint256(-amountOutDelta);
    }

    function _swapBalancerV2(address vault, bytes32 poolId, address tokenIn, address tokenOut, uint256 amountIn)
        private
        returns (uint256 amountOut)
    {
        uint256 beforeOut = _balanceOf(tokenOut, address(this));
        _approveToken(tokenIn, vault, amountIn);
        IBalancerV2VaultMinimal(vault).swap(
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
            type(uint256).max
        );
        amountOut = _balanceOf(tokenOut, address(this)) - beforeOut;
    }

    function _swapCurve(
        address pool,
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 i,
        uint256 j,
        uint8 protocol
    ) private returns (uint256 amountOut) {
        uint256 beforeOut = _balanceOf(tokenOut, address(this));
        _approveToken(tokenIn, pool, amountIn);
        if (protocol == PROTOCOL_CURVE_STABLE) {
            _callCurve4(pool, 0x3df02124, i, j, amountIn);
        } else if (protocol == PROTOCOL_CURVE_CRYPTO) {
            _callCurve4(pool, 0x5b41b908, i, j, amountIn);
        } else {
            _callCurve5(pool, 0x394747c5, i, j, amountIn);
        }
        amountOut = _balanceOf(tokenOut, address(this)) - beforeOut;
    }

    function _genericCall(
        bytes calldata route,
        uint256 offset,
        address target,
        address tokenIn,
        address tokenOut,
        uint256 amountIn
    ) private returns (uint256 amountOut, uint256 nextOffset) {
        _requireRoute(route, offset, 28);
        address spender = _readAddress(route, offset);
        uint256 amountInOffset = _readUint32(route, offset + 20);
        uint256 dataLen = _readUint32(route, offset + 24);
        nextOffset = offset + 28;
        _requireRoute(route, nextOffset, dataLen);

        uint256 beforeOut = _balanceOf(tokenOut, address(this));
        if (spender != address(0)) {
            _approveToken(tokenIn, spender, amountIn);
        }
        bytes memory data = route[nextOffset:nextOffset + dataLen];
        if (amountInOffset != UINT32_NONE) {
            _patchAmount(data, amountInOffset, amountIn);
        }
        (bool success, bytes memory reason) = target.call(data);
        if (!success) revert GenericCallFailed(target, reason);
        amountOut = _balanceOf(tokenOut, address(this)) - beforeOut;
        nextOffset += dataLen;
    }

    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external {
        _v3PayCallback(amount0Delta, amount1Delta, data);
    }

    function pancakeV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external {
        _v3PayCallback(amount0Delta, amount1Delta, data);
    }

    function _v3PayCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) private {
        if (data.length != 40) revert InvalidCallback(msg.sender, address(0));
        address expectedPool;
        address tokenIn;
        assembly ("memory-safe") {
            expectedPool := shr(96, calldataload(data.offset))
            tokenIn := shr(96, calldataload(add(data.offset, 20)))
        }
        if (msg.sender != expectedPool) revert InvalidCallback(msg.sender, expectedPool);
        uint256 amountToPay = amount0Delta > 0 ? uint256(amount0Delta) : uint256(amount1Delta);
        tokenIn.safeTransfer(msg.sender, amountToPay);
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

    function _solidlyAmountOut(address pool, uint256 amountIn, address tokenIn) private view returns (uint256 amountOut) {
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

    function _callV3Swap(address pool, address tokenIn, bool zeroForOne, uint256 amountIn)
        private
        returns (int256 amount0, int256 amount1)
    {
        uint160 sqrtPriceLimitX96 = zeroForOne ? MIN_SQRT_RATIO : MAX_SQRT_RATIO;
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x128acb08))
            mstore(add(ptr, 0x04), address())
            mstore(add(ptr, 0x24), zeroForOne)
            mstore(add(ptr, 0x44), amountIn)
            mstore(add(ptr, 0x64), sqrtPriceLimitX96)
            mstore(add(ptr, 0x84), 0xa0)
            mstore(add(ptr, 0xa4), 40)
            mstore(add(ptr, 0xc4), shl(96, pool))
            mstore(add(ptr, 0xd8), shl(96, tokenIn))
            mstore(add(ptr, 0xf8), 0)
            if iszero(call(gas(), pool, 0, ptr, 0x104, ptr, 0x40)) {
                returndatacopy(0, 0, returndatasize())
                revert(0, returndatasize())
            }
            amount0 := mload(ptr)
            amount1 := mload(add(ptr, 0x20))
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

    function _approveToken(address token, address spender, uint256 amount) private {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x095ea7b3))
            mstore(add(ptr, 0x04), spender)
            mstore(add(ptr, 0x24), amount)
            let success := call(gas(), token, 0, ptr, 0x44, ptr, 0x20)
            let ok := and(success, or(iszero(returndatasize()), eq(mload(ptr), 1)))

            if iszero(ok) {
                mstore(add(ptr, 0x24), 0)
                pop(call(gas(), token, 0, ptr, 0x44, 0, 0))
                mstore(add(ptr, 0x24), amount)
                success := call(gas(), token, 0, ptr, 0x44, ptr, 0x20)
                ok := and(success, or(iszero(returndatasize()), eq(mload(ptr), 1)))
            }

            if iszero(ok) {
                mstore(0x00, 0x1b6c83ab)
                mstore(0x20, token)
                mstore(0x40, spender)
                revert(0x1c, 0x44)
            }
        }
    }

    function _patchAmount(bytes memory data, uint256 amountInOffset, uint256 amountIn) private pure {
        if (data.length < amountInOffset + 32) revert InvalidAmountPatch(amountInOffset, data.length);
        assembly ("memory-safe") {
            mstore(add(add(data, 32), amountInOffset), amountIn)
        }
    }

    function _getAmountOut(uint256 amountIn, uint256 reserveIn, uint256 reserveOut) private pure returns (uint256) {
        uint256 amountInWithFee = amountIn * 997;
        return (amountInWithFee * reserveOut) / (reserveIn * 1000 + amountInWithFee);
    }

    function _requireRoute(bytes calldata route, uint256 offset, uint256 len) private pure {
        if (offset > route.length || route.length - offset < len) revert InvalidPackedRoute();
    }

    function _readUint8(bytes calldata data, uint256 offset) private pure returns (uint8 value) {
        assembly ("memory-safe") {
            value := byte(0, calldataload(add(data.offset, offset)))
        }
    }

    function _readUint32(bytes calldata data, uint256 offset) private pure returns (uint32 value) {
        assembly ("memory-safe") {
            value := shr(224, calldataload(add(data.offset, offset)))
        }
    }

    function _readBytes32(bytes calldata data, uint256 offset) private pure returns (bytes32 value) {
        assembly ("memory-safe") {
            value := calldataload(add(data.offset, offset))
        }
    }

    function _readAddress(bytes calldata data, uint256 offset) private pure returns (address value) {
        assembly ("memory-safe") {
            value := shr(96, calldataload(add(data.offset, offset)))
        }
    }
}
