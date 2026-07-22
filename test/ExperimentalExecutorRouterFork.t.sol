// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

import {ExperimentalExecutorRouter} from "../contracts/ExperimentalExecutorRouter.sol";

interface VmFork {
    function createSelectFork(string calldata rpcUrl, uint256 blockNumber) external returns (uint256 forkId);
    function deal(address account, uint256 newBalance) external;
    function addr(uint256 privateKey) external returns (address);
    function sign(uint256 privateKey, bytes32 digest) external returns (uint8 v, bytes32 r, bytes32 s);
    function prank(address sender) external;
    function envOr(string calldata name, string calldata defaultValue) external view returns (string memory value);
    function skip(bool skipTest) external;
}

interface IERC20Fork {
    function approve(address spender, uint256 amount) external returns (bool);
    function allowance(address owner, address spender) external view returns (uint256);
    function balanceOf(address owner) external view returns (uint256);
}

interface IWETHFork is IERC20Fork {
    function deposit() external payable;
}

interface IERC2612Fork is IERC20Fork {
    function DOMAIN_SEPARATOR() external view returns (bytes32);
    function nonces(address owner) external view returns (uint256);
}

interface IPermit2Fork {
    function DOMAIN_SEPARATOR() external view returns (bytes32);
    function nonceBitmap(address owner, uint256 wordPos) external view returns (uint256);
}

/// @dev Opt-in archive-RPC checks against Ethereum block 21,000,000. The
/// sidecar script requires ETHEREUM_RPC_URL; ordinary local test runs skip.
contract ExperimentalExecutorRouterForkTest {
    VmFork private constant vm = VmFork(address(uint160(uint256(keccak256("hevm cheat code")))));

    uint256 private constant PINNED_BLOCK = 21_000_000;
    address private constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address private constant DAI = 0x6B175474E89094C44Da98b954EedeAC495271d0F;
    address private constant BAL = 0xba100000625a3754423978a60c9317c58a424e3D;
    address private constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address private constant PERMIT2 = 0x000000000022D473030F116dDEE9F6B43aC78BA3;
    address private constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address private constant V2_USDC_WETH = 0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc;
    address private constant V2_DAI_WETH = 0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11;
    address private constant V3_USDC_WETH_005 = 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640;
    address private constant PANCAKE_V3_USDC_WETH_005 = 0x1ac1A8FEaAEa1900C4166dEeed0C11cC10669D36;
    address private constant CURVE_3POOL = 0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7;
    bytes32 private constant BALANCER_BAL_WETH_8020 =
        0x5c6ee304399dbdb9c8ef030ab642b10820db8f56000200000000000000000014;

    struct RouterBalances {
        uint256 eth;
        uint256 first;
        uint256 second;
        uint256 third;
    }

    function testFork_realUniswapV2AllowanceRoute() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 amountIn = 0.1 ether;
        vm.deal(address(this), amountIn);
        IWETHFork(WETH).deposit{value: amountIn}();
        IERC20Fork(WETH).approve(address(router), amountIn);
        uint256 beforeOut = IERC20Fork(USDC).balanceOf(address(this));
        RouterBalances memory beforeRouter = _balances(router, WETH, DAI, USDC);

        uint256 amountOut = router.executeExactInput(
            _params(WETH, USDC, amountIn, abi.encodePacked(USDC, uint8(0), V2_USDC_WETH, WETH, USDC, uint16(30)))
        );

        require(amountOut > 0, "V2 produced no output");
        require(IERC20Fork(USDC).balanceOf(address(this)) - beforeOut == amountOut, "V2 output mismatch");
        require(IERC20Fork(WETH).allowance(address(this), address(router)) == 0, "router allowance remains");
        _requireRestored(router, WETH, DAI, USDC, beforeRouter);
    }

    function testFork_realUniswapV3NativeRoute() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 amountIn = 0.1 ether;
        vm.deal(address(this), amountIn);
        uint256 beforeOut = IERC20Fork(USDC).balanceOf(address(this));
        RouterBalances memory beforeRouter = _balances(router, WETH, DAI, USDC);

        uint256 amountOut = router.executeExactInputNative{value: amountIn}(
            _params(WETH, USDC, amountIn, abi.encodePacked(USDC, uint8(1), V3_USDC_WETH_005, WETH, USDC))
        );

        require(amountOut > 0, "V3 produced no output");
        require(IERC20Fork(USDC).balanceOf(address(this)) - beforeOut == amountOut, "V3 output mismatch");
        _requireRestored(router, WETH, DAI, USDC, beforeRouter);
    }

    function testFork_realPancakeV3NativeRoute() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 amountIn = 0.1 ether;
        vm.deal(address(this), amountIn);
        uint256 beforeOut = IERC20Fork(USDC).balanceOf(address(this));
        RouterBalances memory beforeRouter = _balances(router, WETH, DAI, USDC);

        uint256 amountOut = router.executeExactInputNative{value: amountIn}(
            _params(WETH, USDC, amountIn, abi.encodePacked(USDC, uint8(2), PANCAKE_V3_USDC_WETH_005, WETH, USDC))
        );

        require(amountOut > 0, "Pancake V3 produced no output");
        require(IERC20Fork(USDC).balanceOf(address(this)) - beforeOut == amountOut, "Pancake V3 output mismatch");
        _requireRestored(router, WETH, DAI, USDC, beforeRouter);
    }

    function testFork_realBalancerV2NativeRoute() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 amountIn = 0.1 ether;
        vm.deal(address(this), amountIn);
        uint256 beforeOut = IERC20Fork(BAL).balanceOf(address(this));
        RouterBalances memory beforeRouter = _balances(router, WETH, BAL, USDC);

        uint256 amountOut = router.executeExactInputNative{value: amountIn}(
            _params(
                WETH, BAL, amountIn, abi.encodePacked(BAL, uint8(5), BALANCER_VAULT, WETH, BAL, BALANCER_BAL_WETH_8020)
            )
        );

        require(amountOut > 0, "Balancer V2 produced no output");
        require(IERC20Fork(BAL).balanceOf(address(this)) - beforeOut == amountOut, "Balancer V2 output mismatch");
        require(IERC20Fork(WETH).allowance(address(router), BALANCER_VAULT) == 0, "Balancer approval remains");
        _requireRestored(router, WETH, BAL, USDC, beforeRouter);
    }

    function testFork_realMixedUniswapV2CurveRoute() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 amountIn = 0.1 ether;
        vm.deal(address(this), amountIn);
        uint256 beforeOut = IERC20Fork(USDC).balanceOf(address(this));
        RouterBalances memory beforeRouter = _balances(router, WETH, DAI, USDC);
        bytes memory route = abi.encodePacked(
            USDC, uint8(0), V2_DAI_WETH, WETH, DAI, uint16(30), uint8(6), CURVE_3POOL, DAI, USDC, uint8(0), uint8(1)
        );

        uint256 amountOut = router.executeExactInputNative{value: amountIn}(_params(WETH, USDC, amountIn, route));

        require(amountOut > 0, "mixed route produced no output");
        require(IERC20Fork(USDC).balanceOf(address(this)) - beforeOut == amountOut, "mixed output mismatch");
        require(IERC20Fork(DAI).allowance(address(router), CURVE_3POOL) == 0, "Curve approval remains");
        _requireRestored(router, WETH, DAI, USDC, beforeRouter);
    }

    function testFork_realUsdcErc2612PermitAndSwap() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 ownerKey = 0xA11CE;
        address owner = vm.addr(ownerKey);
        uint256 amountIn = _buyUsdc(router, owner, 0.1 ether);
        uint256 deadline = block.timestamp + 60;
        uint256 nonce = IERC2612Fork(USDC).nonces(owner);
        bytes32 permitTypehash =
            keccak256("Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)");
        bytes32 structHash = keccak256(abi.encode(permitTypehash, owner, address(router), amountIn, nonce, deadline));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", IERC2612Fork(USDC).DOMAIN_SEPARATOR(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(ownerKey, digest);
        ExperimentalExecutorRouter.ERC2612Permit memory permit =
            ExperimentalExecutorRouter.ERC2612Permit({v: v, r: r, s: s});
        ExperimentalExecutorRouter.ExactInputParams memory params = _paramsTo(
            USDC, WETH, amountIn, owner, abi.encodePacked(WETH, uint8(0), V2_USDC_WETH, USDC, WETH, uint16(30))
        );
        params.deadline = deadline;
        uint256 beforeOut = IERC20Fork(WETH).balanceOf(owner);
        RouterBalances memory beforeRouter = _balances(router, WETH, DAI, USDC);

        vm.prank(owner);
        uint256 amountOut = router.executeExactInputWithPermit(params, permit);

        require(amountOut > 0, "ERC-2612 swap produced no output");
        require(IERC20Fork(WETH).balanceOf(owner) - beforeOut == amountOut, "ERC-2612 output mismatch");
        require(IERC2612Fork(USDC).nonces(owner) == nonce + 1, "ERC-2612 nonce was not consumed");
        require(IERC20Fork(USDC).allowance(owner, address(router)) == 0, "ERC-2612 allowance remains");
        _requireRestored(router, WETH, DAI, USDC, beforeRouter);
    }

    function testFork_realPermit2SignatureTransferAndSwap() external {
        if (!_selectFork()) return;
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(WETH, PERMIT2);
        uint256 ownerKey = 0xB0B;
        address owner = vm.addr(ownerKey);
        uint256 amountIn = 0.1 ether;
        vm.deal(owner, amountIn);
        vm.prank(owner);
        IWETHFork(WETH).deposit{value: amountIn}();
        vm.prank(owner);
        IERC20Fork(WETH).approve(PERMIT2, amountIn);

        uint256 nonce = 0x1234;
        uint256 deadline = block.timestamp + 60;
        bytes32 tokenPermissionsTypehash = keccak256("TokenPermissions(address token,uint256 amount)");
        bytes32 permitTransferFromTypehash = keccak256(
            "PermitTransferFrom(TokenPermissions permitted,address spender,uint256 nonce,uint256 deadline)TokenPermissions(address token,uint256 amount)"
        );
        bytes32 tokenPermissionsHash = keccak256(abi.encode(tokenPermissionsTypehash, WETH, amountIn));
        bytes32 structHash =
            keccak256(abi.encode(permitTransferFromTypehash, tokenPermissionsHash, address(router), nonce, deadline));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", IPermit2Fork(PERMIT2).DOMAIN_SEPARATOR(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(ownerKey, digest);
        ExperimentalExecutorRouter.Permit2SignatureTransfer memory permit =
            ExperimentalExecutorRouter.Permit2SignatureTransfer({
                nonce: nonce, deadline: deadline, signature: abi.encodePacked(r, s, v)
            });
        ExperimentalExecutorRouter.ExactInputParams memory params = _paramsTo(
            WETH, USDC, amountIn, owner, abi.encodePacked(USDC, uint8(0), V2_USDC_WETH, WETH, USDC, uint16(30))
        );
        params.deadline = deadline;
        uint256 beforeOut = IERC20Fork(USDC).balanceOf(owner);
        RouterBalances memory beforeRouter = _balances(router, WETH, DAI, USDC);

        vm.prank(owner);
        uint256 amountOut = router.executeExactInputWithPermit2(params, permit);

        require(amountOut > 0, "Permit2 swap produced no output");
        require(IERC20Fork(USDC).balanceOf(owner) - beforeOut == amountOut, "Permit2 output mismatch");
        require(IERC20Fork(WETH).allowance(owner, PERMIT2) == 0, "Permit2 token allowance remains");
        uint256 nonceWord = IPermit2Fork(PERMIT2).nonceBitmap(owner, nonce >> 8);
        uint256 nonceBit = nonce & 0xff;
        require(nonceWord & (uint256(1) << nonceBit) != 0, "Permit2 nonce was not consumed");
        _requireRestored(router, WETH, DAI, USDC, beforeRouter);
    }

    function _selectFork() private returns (bool selected) {
        string memory rpcUrl = vm.envOr("ETHEREUM_RPC_URL", string(""));
        if (bytes(rpcUrl).length == 0) {
            vm.skip(true);
            return false;
        }
        vm.createSelectFork(rpcUrl, PINNED_BLOCK);
        return true;
    }

    function _params(address tokenIn, address tokenOut, uint256 amountIn, bytes memory route)
        private
        view
        returns (ExperimentalExecutorRouter.ExactInputParams memory)
    {
        return _paramsTo(tokenIn, tokenOut, amountIn, address(this), route);
    }

    function _paramsTo(address tokenIn, address tokenOut, uint256 amountIn, address recipient, bytes memory route)
        private
        view
        returns (ExperimentalExecutorRouter.ExactInputParams memory)
    {
        return ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: tokenIn,
            tokenOut: tokenOut,
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: recipient,
            deadline: block.timestamp + 60,
            route: route
        });
    }

    function _buyUsdc(ExperimentalExecutorRouter router, address owner, uint256 ethAmount)
        private
        returns (uint256 amountOut)
    {
        vm.deal(owner, ethAmount);
        uint256 beforeOut = IERC20Fork(USDC).balanceOf(owner);
        vm.prank(owner);
        router.executeExactInputNative{value: ethAmount}(
            _paramsTo(
                WETH, USDC, ethAmount, owner, abi.encodePacked(USDC, uint8(0), V2_USDC_WETH, WETH, USDC, uint16(30))
            )
        );
        amountOut = IERC20Fork(USDC).balanceOf(owner) - beforeOut;
        require(amountOut > 0, "USDC setup swap produced no output");
    }

    function _balances(ExperimentalExecutorRouter router, address first, address second, address third)
        private
        view
        returns (RouterBalances memory balances)
    {
        balances = RouterBalances({
            eth: address(router).balance,
            first: IERC20Fork(first).balanceOf(address(router)),
            second: IERC20Fork(second).balanceOf(address(router)),
            third: IERC20Fork(third).balanceOf(address(router))
        });
    }

    function _requireRestored(
        ExperimentalExecutorRouter router,
        address first,
        address second,
        address third,
        RouterBalances memory beforeRouter
    ) private view {
        RouterBalances memory afterRouter = _balances(router, first, second, third);
        require(afterRouter.eth == beforeRouter.eth, "router ETH balance changed");
        require(afterRouter.first == beforeRouter.first, "router first-token balance changed");
        require(afterRouter.second == beforeRouter.second, "router second-token balance changed");
        require(afterRouter.third == beforeRouter.third, "router third-token balance changed");
    }
}
