// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

import {ExperimentalExecutorRouter} from "../contracts/ExperimentalExecutorRouter.sol";
import {ExperimentalExecutorRouterFactory} from "../contracts/ExperimentalExecutorRouterFactory.sol";

interface Vm {
    function deal(address account, uint256 newBalance) external;

    function addr(uint256 privateKey) external returns (address);

    function sign(uint256 privateKey, bytes32 digest) external returns (uint8 v, bytes32 r, bytes32 s);

    function prank(address sender) external;
}

contract ExperimentalExecutorRouterTest {
    Vm private constant vm = Vm(address(uint160(uint256(keccak256("hevm cheat code")))));

    struct V2Scenario {
        MockERC20 tokenIn;
        MockERC20 tokenOut;
        MockV2Pair pair;
        ExperimentalExecutorRouter router;
        uint256 reserve;
    }

    function test_exactInputV2ConsumesEntireInputAndReturnsOutput() external {
        V2Scenario memory scenario = _newV2Scenario();

        uint256 amountIn = 10 ether;
        uint256 expectedOut = _v2AmountOut(amountIn, scenario.reserve);
        address recipient = address(0xA11CE);
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);

        bytes memory route = abi.encodePacked(
            address(scenario.tokenOut),
            uint8(0),
            address(scenario.pair),
            address(scenario.tokenIn),
            address(scenario.tokenOut),
            uint16(30)
        );
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(scenario.tokenIn),
            tokenOut: address(scenario.tokenOut),
            amountIn: amountIn,
            minAmountOut: expectedOut,
            recipient: recipient,
            deadline: block.timestamp + 1,
            route: route
        });

        uint256 amountOut = scenario.router.executeExactInput(params);

        require(amountOut == expectedOut, "unexpected amount out");
        require(scenario.tokenOut.balanceOf(recipient) == expectedOut, "recipient did not receive output");
        require(scenario.tokenIn.balanceOf(address(scenario.router)) == 0, "router retained input");
        require(scenario.tokenOut.balanceOf(address(scenario.router)) == 0, "router retained output");
    }

    function testFuzz_v2RestoresPrefundedRouterBalances(
        uint96 amountSeed,
        uint96 inputDustSeed,
        uint96 outputDustSeed,
        uint96 ethDustSeed
    ) external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = uint256(amountSeed) % (1_000 ether) + 1 ether;
        uint256 inputDust = uint256(inputDustSeed) % (100 ether);
        uint256 outputDust = uint256(outputDustSeed) % (100 ether);
        uint256 ethDust = uint256(ethDustSeed) % (100 ether);
        uint256 expectedOut = _v2AmountOut(amountIn, scenario.reserve);
        address recipient = address(0xA11CE);

        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.mint(address(scenario.router), inputDust);
        scenario.tokenOut.mint(address(scenario.router), outputDust);
        vm.deal(address(scenario.router), ethDust);
        scenario.tokenIn.approve(address(scenario.router), amountIn);

        uint256 amountOut = scenario.router.executeExactInput(_v2Params(scenario, amountIn, expectedOut, recipient));

        require(amountOut == expectedOut, "unexpected fuzz output");
        require(scenario.tokenOut.balanceOf(recipient) == expectedOut, "recipient fuzz output mismatch");
        require(scenario.tokenIn.balanceOf(address(scenario.router)) == inputDust, "input baseline changed");
        require(scenario.tokenOut.balanceOf(address(scenario.router)) == outputDust, "output baseline changed");
        require(address(scenario.router).balance == ethDust, "ETH baseline changed");
    }

    function test_uniswapV2UsesEncodedFee() external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = 10 ether;
        uint16 feeBps = 100;
        uint256 expectedOut = _v2AmountOutWithFee(amountIn, scenario.reserve, feeBps);
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(scenario.tokenIn),
            tokenOut: address(scenario.tokenOut),
            amountIn: amountIn,
            minAmountOut: expectedOut,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(scenario.tokenOut),
                uint8(0),
                address(scenario.pair),
                address(scenario.tokenIn),
                address(scenario.tokenOut),
                feeBps
            )
        });

        uint256 amountOut = scenario.router.executeExactInput(params);

        require(amountOut == expectedOut, "encoded V2 fee was not used");
    }

    function test_revertsWhenFinalOutputIsBelowMinOut() external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = 10 ether;
        uint256 expectedOut = _v2AmountOut(amountIn, scenario.reserve);
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params =
            _v2Params(scenario, amountIn, expectedOut + 1, address(0xA11CE));

        (bool success, bytes memory result) =
            address(scenario.router).call(abi.encodeCall(scenario.router.executeExactInput, (params)));

        require(!success, "min-out violation succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.InsufficientOutput.selector, "wrong revert");
    }

    function test_rejectsZeroRecipientBeforePullingFunds() external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = 10 ether;
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = _v2Params(scenario, amountIn, 1, address(0));

        (bool success, bytes memory result) =
            address(scenario.router).call(abi.encodeCall(scenario.router.executeExactInput, (params)));

        require(!success, "zero recipient succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.ZeroRecipient.selector, "wrong revert");
        require(scenario.tokenIn.balanceOf(address(this)) == amountIn, "funds were pulled");
    }

    function test_rejectsSameTokenRouteBeforePullingFunds() external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = 10 ether;
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = _v2Params(scenario, amountIn, 1, address(0xA11CE));
        params.tokenOut = params.tokenIn;

        (bool success, bytes memory result) =
            address(scenario.router).call(abi.encodeCall(scenario.router.executeExactInput, (params)));

        require(!success, "same-token route succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.InvalidTokenPair.selector, "wrong revert");
        require(scenario.tokenIn.balanceOf(address(this)) == amountIn, "funds were pulled");
    }

    function test_feeOnTransferInputCannotConsumePrefundedRouterBalance() external {
        FeeOnTransferERC20 tokenIn = new FeeOnTransferERC20("Taxed Input", "TIN", 1_000);
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        (MockERC20 token0, MockERC20 token1) = address(tokenIn) < address(tokenOut)
            ? (MockERC20(address(tokenIn)), tokenOut)
            : (tokenOut, MockERC20(address(tokenIn)));
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 amountIn = 10 ether;
        uint256 routerDust = 1 ether;
        tokenIn.mint(address(this), amountIn);
        tokenIn.mint(address(router), routerDust);
        tokenIn.mint(address(pair), 1_000_000 ether);
        tokenOut.mint(address(pair), 1_000_000 ether);
        pair.sync();
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });

        (bool success, bytes memory result) = address(router).call(abi.encodeCall(router.executeExactInput, (params)));

        require(!success, "taxed input succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.InvalidInputAmount.selector, "wrong revert");
        require(tokenIn.balanceOf(address(router)) == routerDust, "router dust changed");
        require(tokenIn.balanceOf(address(this)) == amountIn, "caller funds changed");
    }

    function test_feeOnTransferOutputCannotBypassRecipientMinimum() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        FeeOnTransferERC20 tokenOut = new FeeOnTransferERC20("Taxed Output", "TOUT", 1_000);
        (MockERC20 token0, MockERC20 token1) = address(tokenIn) < address(tokenOut)
            ? (tokenIn, MockERC20(address(tokenOut)))
            : (MockERC20(address(tokenOut)), tokenIn);
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 amountIn = 10 ether;
        tokenIn.mint(address(this), amountIn);
        tokenIn.mint(address(pair), 1_000_000 ether);
        tokenOut.mint(address(pair), 1_000_000 ether);
        pair.sync();
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });

        (bool success, bytes memory result) = address(router).call(abi.encodeCall(router.executeExactInput, (params)));

        require(!success, "taxed recipient output succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.InvalidRecipientOutput.selector, "wrong revert");
        require(tokenIn.balanceOf(address(this)) == amountIn, "caller funds changed");
        require(tokenOut.balanceOf(address(0xA11CE)) == 0, "recipient retained taxed output");
    }

    function test_rejectsExpiredSwapBeforePullingFunds() external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = 10 ether;
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = _v2Params(scenario, amountIn, 1, address(0xA11CE));
        params.deadline = block.timestamp - 1;

        (bool success, bytes memory result) =
            address(scenario.router).call(abi.encodeCall(scenario.router.executeExactInput, (params)));

        require(!success, "expired swap succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.Expired.selector, "wrong revert");
        require(scenario.tokenIn.balanceOf(address(this)) == amountIn, "funds were pulled");
    }

    function test_blocksReentrancyFromInputToken() external {
        ReentrantERC20 tokenIn = new ReentrantERC20();
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        (MockERC20 token0, MockERC20 token1) = address(tokenIn) < address(tokenOut)
            ? (MockERC20(address(tokenIn)), tokenOut)
            : (tokenOut, MockERC20(address(tokenIn)));
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 reserve = 1_000_000 ether;
        tokenIn.mint(address(pair), reserve);
        tokenOut.mint(address(pair), reserve);
        pair.sync();

        uint256 amountIn = 10 ether;
        tokenIn.mint(address(this), amountIn);
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });
        tokenIn.configure(address(router), abi.encodeCall(router.executeExactInput, (params)));

        router.executeExactInput(params);

        require(tokenIn.reentryBlocked(), "nested swap was not blocked");
        require(
            tokenIn.reentrySelector() == ExperimentalExecutorRouter.Reentrancy.selector,
            "nested swap was blocked for the wrong reason"
        );
        require(tokenIn.balanceOf(address(router)) == 0, "router retained input");
        require(tokenOut.balanceOf(address(router)) == 0, "router retained output");
    }

    function test_nativeInputWrapsAndConsumesAllEth() external {
        MockWETH weth = new MockWETH();
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        (MockERC20 token0, MockERC20 token1) = address(weth) < address(tokenOut)
            ? (MockERC20(address(weth)), tokenOut)
            : (tokenOut, MockERC20(address(weth)));
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(weth), address(0xCAFE));
        uint256 reserve = 1_000_000 ether;
        weth.mint(address(pair), reserve);
        tokenOut.mint(address(pair), reserve);
        pair.sync();

        uint256 amountIn = 10 ether;
        uint256 expectedOut = _v2AmountOut(amountIn, reserve);
        vm.deal(address(this), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(weth),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: expectedOut,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(weth), address(tokenOut), uint16(30)
            )
        });

        uint256 amountOut = router.executeExactInputNative{value: amountIn}(params);

        require(amountOut == expectedOut, "unexpected amount out");
        require(address(router).balance == 0, "router retained ETH");
        require(weth.balanceOf(address(router)) == 0, "router retained WETH");
        require(tokenOut.balanceOf(address(router)) == 0, "router retained output");
    }

    function test_erc2612PermitAndSwapAreAtomic() external {
        MockPermitERC20 tokenIn = new MockPermitERC20();
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        (MockERC20 token0, MockERC20 token1) = address(tokenIn) < address(tokenOut)
            ? (MockERC20(address(tokenIn)), tokenOut)
            : (tokenOut, MockERC20(address(tokenIn)));
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 reserve = 1_000_000 ether;
        tokenIn.mint(address(pair), reserve);
        tokenOut.mint(address(pair), reserve);
        pair.sync();

        uint256 ownerKey = 0xA11CE;
        address owner = vm.addr(ownerKey);
        uint256 amountIn = 10 ether;
        uint256 deadline = block.timestamp + 1;
        tokenIn.mint(owner, amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: owner,
            deadline: deadline,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });
        bytes32 digest = tokenIn.permitDigest(owner, address(router), amountIn, deadline);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(ownerKey, digest);
        ExperimentalExecutorRouter.ERC2612Permit memory permit =
            ExperimentalExecutorRouter.ERC2612Permit({v: v, r: r, s: s});

        vm.prank(owner);
        router.executeExactInputWithPermit(params, permit);

        require(tokenIn.nonces(owner) == 1, "permit nonce was not consumed");
        require(tokenIn.allowance(owner, address(router)) == 0, "permit allowance remains");
        require(tokenOut.balanceOf(owner) != 0, "owner did not receive output");
    }

    function test_invalidErc2612PermitLeavesEveryBalanceAndNonceUnchanged() external {
        MockPermitERC20 tokenIn = new MockPermitERC20();
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        (MockERC20 token0, MockERC20 token1) = address(tokenIn) < address(tokenOut)
            ? (MockERC20(address(tokenIn)), tokenOut)
            : (tokenOut, MockERC20(address(tokenIn)));
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        tokenIn.mint(address(pair), 1_000_000 ether);
        tokenOut.mint(address(pair), 1_000_000 ether);
        pair.sync();

        uint256 ownerKey = 0xA11CE;
        address owner = vm.addr(ownerKey);
        uint256 amountIn = 10 ether;
        uint256 deadline = block.timestamp + 1;
        tokenIn.mint(owner, amountIn);
        tokenIn.mint(address(router), 3 ether);
        tokenOut.mint(address(router), 5 ether);
        vm.deal(address(router), 7 ether);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: owner,
            deadline: deadline,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });
        bytes32 digest = tokenIn.permitDigest(owner, address(router), amountIn, deadline);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(0xBAD, digest);
        ExperimentalExecutorRouter.ERC2612Permit memory permit =
            ExperimentalExecutorRouter.ERC2612Permit({v: v, r: r, s: s});

        vm.prank(owner);
        (bool success,) = address(router).call(abi.encodeCall(router.executeExactInputWithPermit, (params, permit)));

        require(!success, "invalid permit succeeded");
        require(tokenIn.nonces(owner) == 0, "failed permit consumed nonce");
        require(tokenIn.balanceOf(owner) == amountIn, "failed permit moved owner funds");
        require(tokenIn.balanceOf(address(router)) == 3 ether, "failed permit changed input baseline");
        require(tokenOut.balanceOf(address(router)) == 5 ether, "failed permit changed output baseline");
        require(address(router).balance == 7 ether, "failed permit changed ETH baseline");
    }

    function test_permit2SignatureTransferAndSwapAreAtomic() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        MockPermit2 permit2 = new MockPermit2();
        (MockERC20 token0, MockERC20 token1) =
            address(tokenIn) < address(tokenOut) ? (tokenIn, tokenOut) : (tokenOut, tokenIn);
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(permit2));
        uint256 reserve = 1_000_000 ether;
        tokenIn.mint(address(pair), reserve);
        tokenOut.mint(address(pair), reserve);
        pair.sync();

        uint256 ownerKey = 0xB0B;
        address owner = vm.addr(ownerKey);
        uint256 amountIn = 10 ether;
        tokenIn.mint(owner, amountIn);
        vm.prank(owner);
        tokenIn.approve(address(permit2), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: owner,
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });
        uint256 nonce = 7;
        uint256 permitDeadline = block.timestamp + 2;
        bytes32 digest =
            permit2.permitDigest(owner, address(tokenIn), amountIn, nonce, permitDeadline, address(router), amountIn);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(ownerKey, digest);
        ExperimentalExecutorRouter.Permit2SignatureTransfer memory permit =
            ExperimentalExecutorRouter.Permit2SignatureTransfer({
                nonce: nonce, deadline: permitDeadline, signature: abi.encodePacked(r, s, v)
            });

        vm.prank(owner);
        router.executeExactInputWithPermit2(params, permit);

        require(permit2.nonceUsed(owner, nonce), "Permit2 nonce was not consumed");
        require(tokenIn.allowance(owner, address(permit2)) == 0, "Permit2 token allowance remains");
        require(tokenOut.balanceOf(owner) != 0, "owner did not receive output");
    }

    function test_invalidPermit2SignatureLeavesEveryBalanceAndNonceUnchanged() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        MockPermit2 permit2 = new MockPermit2();
        (MockERC20 token0, MockERC20 token1) =
            address(tokenIn) < address(tokenOut) ? (tokenIn, tokenOut) : (tokenOut, tokenIn);
        MockV2Pair pair = new MockV2Pair(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(permit2));
        tokenIn.mint(address(pair), 1_000_000 ether);
        tokenOut.mint(address(pair), 1_000_000 ether);
        pair.sync();

        uint256 ownerKey = 0xB0B;
        address owner = vm.addr(ownerKey);
        uint256 amountIn = 10 ether;
        uint256 nonce = 7;
        uint256 deadline = block.timestamp + 2;
        tokenIn.mint(owner, amountIn);
        tokenIn.mint(address(router), 3 ether);
        tokenOut.mint(address(router), 5 ether);
        vm.deal(address(router), 7 ether);
        vm.prank(owner);
        tokenIn.approve(address(permit2), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: owner,
            deadline: deadline,
            route: abi.encodePacked(
                address(tokenOut), uint8(0), address(pair), address(tokenIn), address(tokenOut), uint16(30)
            )
        });
        bytes32 digest =
            permit2.permitDigest(owner, address(tokenIn), amountIn, nonce, deadline, address(router), amountIn);
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(0xBAD, digest);
        ExperimentalExecutorRouter.Permit2SignatureTransfer memory permit =
            ExperimentalExecutorRouter.Permit2SignatureTransfer({
                nonce: nonce, deadline: deadline, signature: abi.encodePacked(r, s, v)
            });

        vm.prank(owner);
        (bool success,) = address(router).call(abi.encodeCall(router.executeExactInputWithPermit2, (params, permit)));

        require(!success, "invalid Permit2 signature succeeded");
        require(!permit2.nonceUsed(owner, nonce), "failed Permit2 consumed nonce");
        require(tokenIn.balanceOf(owner) == amountIn, "failed Permit2 moved owner funds");
        require(tokenIn.balanceOf(address(router)) == 3 ether, "failed Permit2 changed input baseline");
        require(tokenOut.balanceOf(address(router)) == 5 ether, "failed Permit2 changed output baseline");
        require(address(router).balance == 7 ether, "failed Permit2 changed ETH baseline");
    }

    function test_v3ProtocolFamiliesConsumeFullInput() external {
        for (uint8 protocol = 1; protocol <= 3; ++protocol) {
            MockERC20 tokenIn = new MockERC20("Input", "IN");
            MockERC20 tokenOut = new MockERC20("Output", "OUT");
            MockV3Pool pool = new MockV3Pool(tokenIn, tokenOut, protocol == 2);
            ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
            uint256 amountIn = 10 ether;
            uint256 expectedOut = amountIn * 99 / 100;
            tokenOut.mint(address(pool), 1_000_000 ether);
            tokenIn.mint(address(this), amountIn);
            tokenIn.approve(address(router), amountIn);
            ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
                tokenIn: address(tokenIn),
                tokenOut: address(tokenOut),
                amountIn: amountIn,
                minAmountOut: expectedOut,
                recipient: address(0xA11CE),
                deadline: block.timestamp + 1,
                route: abi.encodePacked(address(tokenOut), protocol, address(pool), address(tokenIn), address(tokenOut))
            });

            uint256 amountOut = router.executeExactInput(params);

            require(amountOut == expectedOut, "unexpected V3-family output");
            require(tokenIn.balanceOf(address(router)) == 0, "router retained V3-family input");
            require(tokenOut.balanceOf(address(router)) == 0, "router retained V3-family output");
        }
    }

    function test_solidlyV2ConsumesFullInput() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        (MockERC20 token0, MockERC20 token1) =
            address(tokenIn) < address(tokenOut) ? (tokenIn, tokenOut) : (tokenOut, tokenIn);
        MockSolidlyV2Pool pool = new MockSolidlyV2Pool(token0, token1);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 amountIn = 10 ether;
        uint256 expectedOut = amountIn * 99 / 100;
        tokenOut.mint(address(pool), 1_000_000 ether);
        tokenIn.mint(address(this), amountIn);
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: expectedOut,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(address(tokenOut), uint8(4), address(pool), address(tokenIn), address(tokenOut))
        });

        uint256 amountOut = router.executeExactInput(params);

        require(amountOut == expectedOut, "unexpected Solidly output");
        require(tokenIn.balanceOf(address(router)) == 0, "router retained Solidly input");
        require(tokenOut.balanceOf(address(router)) == 0, "router retained Solidly output");
    }

    function test_balancerV2ConsumesFullInputAndClearsApproval() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        MockBalancerV2Vault vault = new MockBalancerV2Vault();
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 amountIn = 10 ether;
        uint256 expectedOut = amountIn * 99 / 100;
        bytes32 poolId = keccak256("pool");
        tokenOut.mint(address(vault), 1_000_000 ether);
        tokenIn.mint(address(this), amountIn);
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: expectedOut,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(5), address(vault), address(tokenIn), address(tokenOut), poolId
            )
        });

        uint256 amountOut = router.executeExactInput(params);

        require(amountOut == expectedOut, "unexpected Balancer output");
        require(tokenIn.allowance(address(router), address(vault)) == 0, "Balancer approval remains");
        require(tokenIn.balanceOf(address(router)) == 0, "router retained Balancer input");
        require(tokenOut.balanceOf(address(router)) == 0, "router retained Balancer output");
    }

    function test_curveFamiliesConsumeFullInputAndClearApproval() external {
        for (uint8 protocol = 6; protocol <= 8; ++protocol) {
            MockERC20 tokenIn = new MockERC20("Input", "IN");
            MockERC20 tokenOut = new MockERC20("Output", "OUT");
            MockCurvePool pool = new MockCurvePool(tokenIn, tokenOut);
            ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
            uint256 amountIn = 10 ether;
            uint256 expectedOut = amountIn * 99 / 100;
            tokenOut.mint(address(pool), 1_000_000 ether);
            tokenIn.mint(address(this), amountIn);
            tokenIn.approve(address(router), amountIn);
            ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
                tokenIn: address(tokenIn),
                tokenOut: address(tokenOut),
                amountIn: amountIn,
                minAmountOut: expectedOut,
                recipient: address(0xA11CE),
                deadline: block.timestamp + 1,
                route: abi.encodePacked(
                    address(tokenOut), protocol, address(pool), address(tokenIn), address(tokenOut), uint8(0), uint8(1)
                )
            });

            uint256 amountOut = router.executeExactInput(params);

            require(amountOut == expectedOut, "unexpected Curve output");
            require(tokenIn.allowance(address(router), address(pool)) == 0, "Curve approval remains");
            require(tokenIn.balanceOf(address(router)) == 0, "router retained Curve input");
            require(tokenOut.balanceOf(address(router)) == 0, "router retained Curve output");
        }
    }

    function test_revertsWhenAHopDoesNotConsumeItsFullInput() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        MockPartialV3Pool pool = new MockPartialV3Pool(tokenIn, tokenOut);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 amountIn = 10 ether;
        tokenOut.mint(address(pool), 1_000_000 ether);
        tokenIn.mint(address(this), amountIn);
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(address(tokenOut), uint8(1), address(pool), address(tokenIn), address(tokenOut))
        });

        (bool success, bytes memory result) = address(router).call(abi.encodeCall(router.executeExactInput, (params)));

        require(!success, "partial hop consumption succeeded");
        require(
            _selector(result) == ExperimentalExecutorRouter.IncompleteHopConsumption.selector,
            "wrong partial-fill revert"
        );
        require(tokenIn.balanceOf(address(this)) == amountIn, "reverted input was not restored");
    }

    function test_rejectsArbitraryExternalCallProtocol() external {
        MockERC20 tokenIn = new MockERC20("Input", "IN");
        MockERC20 tokenOut = new MockERC20("Output", "OUT");
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 amountIn = 10 ether;
        tokenIn.mint(address(this), amountIn);
        tokenIn.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenIn),
            tokenOut: address(tokenOut),
            amountIn: amountIn,
            minAmountOut: 1,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenOut), uint8(255), address(tokenOut), address(tokenIn), address(tokenOut)
            )
        });

        (bool success, bytes memory result) = address(router).call(abi.encodeCall(router.executeExactInput, (params)));

        require(!success, "arbitrary external-call protocol succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.UnsupportedProtocol.selector, "wrong protocol revert");
        require(tokenIn.balanceOf(address(this)) == amountIn, "reverted input was not restored");
    }

    function test_multihopConsumesEachPreviousHopOutputInFull() external {
        MockERC20 tokenA = new MockERC20("Token A", "A");
        MockERC20 tokenB = new MockERC20("Token B", "B");
        MockERC20 tokenC = new MockERC20("Token C", "C");
        MockV2Pair pairAB = _newPair(tokenA, tokenB);
        MockV2Pair pairBC = _newPair(tokenB, tokenC);
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        uint256 reserve = 1_000_000 ether;
        tokenA.mint(address(pairAB), reserve);
        tokenB.mint(address(pairAB), reserve);
        pairAB.sync();
        tokenB.mint(address(pairBC), reserve);
        tokenC.mint(address(pairBC), reserve);
        pairBC.sync();
        uint256 amountIn = 10 ether;
        uint256 firstOut = _v2AmountOut(amountIn, reserve);
        uint256 expectedOut = _v2AmountOut(firstOut, reserve);
        tokenA.mint(address(this), amountIn);
        tokenA.approve(address(router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(tokenA),
            tokenOut: address(tokenC),
            amountIn: amountIn,
            minAmountOut: expectedOut,
            recipient: address(0xA11CE),
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(tokenC),
                uint8(0),
                address(pairAB),
                address(tokenA),
                address(tokenB),
                uint16(30),
                uint8(0),
                address(pairBC),
                address(tokenB),
                address(tokenC),
                uint16(30)
            )
        });

        uint256 amountOut = router.executeExactInput(params);

        require(amountOut == expectedOut, "unexpected multihop output");
        require(tokenA.balanceOf(address(router)) == 0, "router retained first input");
        require(tokenB.balanceOf(address(router)) == 0, "router retained intermediate output");
        require(tokenC.balanceOf(address(router)) == 0, "router retained final output");
    }

    function test_rejectsV3CallbackOutsideAnActivePoolCall() external {
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));

        (bool success, bytes memory result) =
            address(router).call(abi.encodeCall(router.uniswapV3SwapCallback, (int256(1), int256(0), bytes(""))));

        require(!success, "unauthenticated callback succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.InvalidCallback.selector, "wrong callback revert");
    }

    function test_rejectsZeroMinAmountOut() external {
        V2Scenario memory scenario = _newV2Scenario();
        uint256 amountIn = 10 ether;
        scenario.tokenIn.mint(address(this), amountIn);
        scenario.tokenIn.approve(address(scenario.router), amountIn);
        ExperimentalExecutorRouter.ExactInputParams memory params = _v2Params(scenario, amountIn, 0, address(0xA11CE));

        (bool success, bytes memory result) =
            address(scenario.router).call(abi.encodeCall(scenario.router.executeExactInput, (params)));

        require(!success, "zero min-out succeeded");
        require(_selector(result) == ExperimentalExecutorRouter.ZeroMinAmountOut.selector, "wrong min-out revert");
        require(scenario.tokenIn.balanceOf(address(this)) == amountIn, "funds were pulled");
    }

    function test_create2DeploymentIsPredictableAndReusable() external {
        ExperimentalExecutorRouterFactory factory = new ExperimentalExecutorRouterFactory();
        bytes32 salt = keccak256("shared-demo-router-v1");
        address weth = address(0xBEEF);
        address permit2 = address(0xCAFE);
        address predicted = factory.computeAddress(salt, weth, permit2);

        address firstDeployment = factory.deploy(salt, weth, permit2);
        vm.prank(address(0xB0B));
        address secondDeployment = factory.deploy(salt, weth, permit2);

        require(firstDeployment == predicted, "CREATE2 deployment was not predicted");
        require(secondDeployment == firstDeployment, "shared deployment was not reused");
        require(firstDeployment.code.length != 0, "router was not deployed");
        require(ExperimentalExecutorRouter(firstDeployment).WETH() == weth, "wrong deployed WETH");
        require(ExperimentalExecutorRouter(firstDeployment).PERMIT2() == permit2, "wrong deployed Permit2");
    }

    function test_marksContractExperimentalOnChain() external {
        ExperimentalExecutorRouter router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));

        require(
            keccak256(bytes(router.SAFETY_WARNING()))
                == keccak256(bytes("EXPERIMENTAL: NOT INTENDED FOR PUBLIC OR PRODUCTION USE")),
            "missing safety warning"
        );
    }

    function test_rejectsZeroDeploymentDependencies() external {
        try new ExperimentalExecutorRouter(address(0), address(0xCAFE)) {
            revert("zero WETH deployment succeeded");
        } catch (bytes memory result) {
            require(
                _selector(result) == ExperimentalExecutorRouter.ZeroDeploymentDependency.selector,
                "wrong zero WETH revert"
            );
        }

        try new ExperimentalExecutorRouter(address(0xBEEF), address(0)) {
            revert("zero Permit2 deployment succeeded");
        } catch (bytes memory result) {
            require(
                _selector(result) == ExperimentalExecutorRouter.ZeroDeploymentDependency.selector,
                "wrong zero Permit2 revert"
            );
        }
    }

    function _newV2Scenario() internal returns (V2Scenario memory scenario) {
        MockERC20 first = new MockERC20("First", "FIRST");
        MockERC20 second = new MockERC20("Second", "SECOND");
        (scenario.tokenIn, scenario.tokenOut) = address(first) < address(second) ? (first, second) : (second, first);
        scenario.pair = new MockV2Pair(scenario.tokenIn, scenario.tokenOut);
        scenario.router = new ExperimentalExecutorRouter(address(0xBEEF), address(0xCAFE));
        scenario.reserve = 1_000_000 ether;
        scenario.tokenIn.mint(address(scenario.pair), scenario.reserve);
        scenario.tokenOut.mint(address(scenario.pair), scenario.reserve);
        scenario.pair.sync();
    }

    function _newPair(MockERC20 first, MockERC20 second) internal returns (MockV2Pair pair) {
        (MockERC20 token0, MockERC20 token1) = address(first) < address(second) ? (first, second) : (second, first);
        pair = new MockV2Pair(token0, token1);
    }

    function _v2AmountOut(uint256 amountIn, uint256 reserve) internal pure returns (uint256) {
        return amountIn * 997 * reserve / (reserve * 1000 + amountIn * 997);
    }

    function _v2AmountOutWithFee(uint256 amountIn, uint256 reserve, uint16 feeBps) internal pure returns (uint256) {
        uint256 amountInAfterFee = amountIn * (10_000 - feeBps);
        return amountInAfterFee * reserve / (reserve * 10_000 + amountInAfterFee);
    }

    function _v2Params(V2Scenario memory scenario, uint256 amountIn, uint256 minAmountOut, address recipient)
        internal
        view
        returns (ExperimentalExecutorRouter.ExactInputParams memory)
    {
        return ExperimentalExecutorRouter.ExactInputParams({
            tokenIn: address(scenario.tokenIn),
            tokenOut: address(scenario.tokenOut),
            amountIn: amountIn,
            minAmountOut: minAmountOut,
            recipient: recipient,
            deadline: block.timestamp + 1,
            route: abi.encodePacked(
                address(scenario.tokenOut),
                uint8(0),
                address(scenario.pair),
                address(scenario.tokenIn),
                address(scenario.tokenOut),
                uint16(30)
            )
        });
    }

    function _selector(bytes memory result) internal pure returns (bytes4 selector) {
        if (result.length < 4) return bytes4(0);
        assembly ("memory-safe") {
            selector := mload(add(result, 32))
        }
    }
}

contract MockERC20 {
    string public name;
    string public symbol;
    uint8 public constant decimals = 18;
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;

    constructor(string memory name_, string memory symbol_) {
        name = name_;
        symbol = symbol_;
    }

    function mint(address to, uint256 amount) external {
        balanceOf[to] += amount;
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }

    function transfer(address to, uint256 amount) external virtual returns (bool) {
        _transfer(msg.sender, to, amount);
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external virtual returns (bool) {
        uint256 approved = allowance[from][msg.sender];
        require(approved >= amount, "insufficient allowance");
        allowance[from][msg.sender] = approved - amount;
        _transfer(from, to, amount);
        return true;
    }

    function _transfer(address from, address to, uint256 amount) internal {
        require(balanceOf[from] >= amount, "insufficient balance");
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
    }
}

contract ReentrantERC20 is MockERC20 {
    address private target;
    bytes private payload;
    bool private attempted;
    bool public reentryBlocked;
    bytes4 public reentrySelector;

    constructor() MockERC20("Reentrant", "REENTER") {}

    function configure(address target_, bytes memory payload_) external {
        target = target_;
        payload = payload_;
    }

    function transferFrom(address from, address to, uint256 amount) external override returns (bool) {
        uint256 approved = allowance[from][msg.sender];
        require(approved >= amount, "insufficient allowance");
        if (!attempted) {
            attempted = true;
            (bool success, bytes memory result) = target.call(payload);
            reentryBlocked = !success;
            if (result.length >= 4) {
                bytes4 selector;
                assembly ("memory-safe") {
                    selector := mload(add(result, 32))
                }
                reentrySelector = selector;
            }
        }
        allowance[from][msg.sender] = approved - amount;
        _transfer(from, to, amount);
        return true;
    }
}

contract FeeOnTransferERC20 is MockERC20 {
    uint256 private immutable feeBps;

    constructor(string memory name_, string memory symbol_, uint256 feeBps_) MockERC20(name_, symbol_) {
        require(feeBps_ < 10_000, "invalid fee");
        feeBps = feeBps_;
    }

    function transfer(address to, uint256 amount) external override returns (bool) {
        _taxedTransfer(msg.sender, to, amount);
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external override returns (bool) {
        uint256 approved = allowance[from][msg.sender];
        require(approved >= amount, "insufficient allowance");
        allowance[from][msg.sender] = approved - amount;
        _taxedTransfer(from, to, amount);
        return true;
    }

    function _taxedTransfer(address from, address to, uint256 amount) private {
        uint256 fee = amount * feeBps / 10_000;
        _transfer(from, to, amount - fee);
        balanceOf[from] -= fee;
    }
}

contract MockWETH is MockERC20 {
    constructor() MockERC20("Wrapped Ether", "WETH") {}

    function deposit() external payable {
        balanceOf[msg.sender] += msg.value;
    }
}

contract MockPermitERC20 is MockERC20 {
    bytes32 private constant PERMIT_TYPEHASH =
        keccak256("Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)");
    bytes32 public immutable DOMAIN_SEPARATOR;
    mapping(address => uint256) public nonces;

    constructor() MockERC20("Permit Token", "PERMIT") {
        DOMAIN_SEPARATOR = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("Permit Token")),
                keccak256(bytes("1")),
                block.chainid,
                address(this)
            )
        );
    }

    function permitDigest(address owner, address spender, uint256 value, uint256 deadline)
        external
        view
        returns (bytes32)
    {
        return keccak256(
            abi.encodePacked(
                "\x19\x01",
                DOMAIN_SEPARATOR,
                keccak256(abi.encode(PERMIT_TYPEHASH, owner, spender, value, nonces[owner], deadline))
            )
        );
    }

    function permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s)
        external
    {
        require(block.timestamp <= deadline, "permit expired");
        bytes32 digest = keccak256(
            abi.encodePacked(
                "\x19\x01",
                DOMAIN_SEPARATOR,
                keccak256(abi.encode(PERMIT_TYPEHASH, owner, spender, value, nonces[owner]++, deadline))
            )
        );
        require(ecrecover(digest, v, r, s) == owner, "invalid permit");
        allowance[owner][spender] = value;
    }
}

contract MockPermit2 {
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

    mapping(address => mapping(uint256 => bool)) public nonceUsed;

    function permitDigest(
        address owner,
        address token,
        uint256 permittedAmount,
        uint256 nonce,
        uint256 deadline,
        address to,
        uint256 requestedAmount
    ) public view returns (bytes32) {
        return keccak256(
            abi.encode(
                block.chainid, address(this), owner, token, permittedAmount, nonce, deadline, to, requestedAmount
            )
        );
    }

    function permitTransferFrom(
        PermitTransferFrom calldata permit,
        SignatureTransferDetails calldata transferDetails,
        address owner,
        bytes calldata signature
    ) external {
        require(block.timestamp <= permit.deadline, "permit expired");
        require(!nonceUsed[owner][permit.nonce], "nonce used");
        require(transferDetails.requestedAmount <= permit.permitted.amount, "amount exceeds permit");
        bytes32 digest = permitDigest(
            owner,
            permit.permitted.token,
            permit.permitted.amount,
            permit.nonce,
            permit.deadline,
            transferDetails.to,
            transferDetails.requestedAmount
        );
        require(_recover(digest, signature) == owner, "invalid signature");
        nonceUsed[owner][permit.nonce] = true;
        MockERC20(permit.permitted.token).transferFrom(owner, transferDetails.to, transferDetails.requestedAmount);
    }

    function _recover(bytes32 digest, bytes calldata signature) private pure returns (address signer) {
        require(signature.length == 65, "invalid signature length");
        bytes32 r;
        bytes32 s;
        uint8 v;
        assembly ("memory-safe") {
            r := calldataload(signature.offset)
            s := calldataload(add(signature.offset, 32))
            v := byte(0, calldataload(add(signature.offset, 64)))
        }
        signer = ecrecover(digest, v, r, s);
    }
}

contract MockV2Pair {
    MockERC20 public immutable token0;
    MockERC20 public immutable token1;
    uint112 private reserve0;
    uint112 private reserve1;

    constructor(MockERC20 token0_, MockERC20 token1_) {
        token0 = token0_;
        token1 = token1_;
    }

    function sync() external {
        reserve0 = uint112(token0.balanceOf(address(this)));
        reserve1 = uint112(token1.balanceOf(address(this)));
    }

    function getReserves() external view returns (uint112, uint112, uint32) {
        return (reserve0, reserve1, 0);
    }

    function swap(uint256 amount0Out, uint256 amount1Out, address to, bytes calldata) external {
        if (amount0Out != 0) token0.transfer(to, amount0Out);
        if (amount1Out != 0) token1.transfer(to, amount1Out);
        reserve0 = uint112(token0.balanceOf(address(this)));
        reserve1 = uint112(token1.balanceOf(address(this)));
    }
}

interface IV3Callback {
    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external;

    function pancakeV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external;
}

contract MockV3Pool {
    MockERC20 public immutable tokenIn;
    MockERC20 public immutable tokenOut;
    bool public immutable pancake;

    constructor(MockERC20 tokenIn_, MockERC20 tokenOut_, bool pancake_) {
        tokenIn = tokenIn_;
        tokenOut = tokenOut_;
        pancake = pancake_;
    }

    function swap(address recipient, bool zeroForOne, int256 amountSpecified, uint160, bytes calldata data)
        external
        returns (int256 amount0, int256 amount1)
    {
        require(amountSpecified > 0, "not exact input");
        uint256 amountIn = uint256(amountSpecified);
        uint256 amountOut = amountIn * 99 / 100;
        int256 amount0Delta = zeroForOne ? int256(amountIn) : -int256(amountOut);
        int256 amount1Delta = zeroForOne ? -int256(amountOut) : int256(amountIn);
        if (pancake) {
            IV3Callback(msg.sender).pancakeV3SwapCallback(amount0Delta, amount1Delta, data);
        } else {
            IV3Callback(msg.sender).uniswapV3SwapCallback(amount0Delta, amount1Delta, data);
        }
        tokenOut.transfer(recipient, amountOut);
        return (amount0Delta, amount1Delta);
    }
}

contract MockPartialV3Pool {
    MockERC20 public immutable tokenIn;
    MockERC20 public immutable tokenOut;

    constructor(MockERC20 tokenIn_, MockERC20 tokenOut_) {
        tokenIn = tokenIn_;
        tokenOut = tokenOut_;
    }

    function swap(address recipient, bool zeroForOne, int256 amountSpecified, uint160, bytes calldata data)
        external
        returns (int256 amount0, int256 amount1)
    {
        require(amountSpecified > 1, "input too small");
        uint256 amountConsumed = uint256(amountSpecified) - 1;
        uint256 amountOut = amountConsumed * 99 / 100;
        amount0 = zeroForOne ? int256(amountConsumed) : -int256(amountOut);
        amount1 = zeroForOne ? -int256(amountOut) : int256(amountConsumed);
        IV3Callback(msg.sender).uniswapV3SwapCallback(amount0, amount1, data);
        tokenOut.transfer(recipient, amountOut);
    }
}

contract MockSolidlyV2Pool {
    MockERC20 public immutable token0;
    MockERC20 public immutable token1;

    constructor(MockERC20 token0_, MockERC20 token1_) {
        token0 = token0_;
        token1 = token1_;
    }

    function getAmountOut(uint256 amountIn, address) external pure returns (uint256) {
        return amountIn * 99 / 100;
    }

    function swap(uint256 amount0Out, uint256 amount1Out, address recipient, bytes calldata) external {
        if (amount0Out != 0) token0.transfer(recipient, amount0Out);
        if (amount1Out != 0) token1.transfer(recipient, amount1Out);
    }
}

contract MockBalancerV2Vault {
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

    function swap(SingleSwap memory singleSwap, FundManagement memory funds, uint256, uint256)
        external
        returns (uint256 amountCalculated)
    {
        require(singleSwap.kind == SwapKind.GIVEN_IN, "not exact input");
        require(funds.sender == msg.sender, "unexpected sender");
        amountCalculated = singleSwap.amount * 99 / 100;
        MockERC20(singleSwap.assetIn).transferFrom(funds.sender, address(this), singleSwap.amount);
        MockERC20(singleSwap.assetOut).transfer(funds.recipient, amountCalculated);
    }
}

contract MockCurvePool {
    MockERC20 public immutable tokenIn;
    MockERC20 public immutable tokenOut;

    constructor(MockERC20 tokenIn_, MockERC20 tokenOut_) {
        tokenIn = tokenIn_;
        tokenOut = tokenOut_;
    }

    function exchange(int128, int128, uint256 amountIn, uint256) external returns (uint256) {
        return _exchange(amountIn, msg.sender);
    }

    function exchange(uint256, uint256, uint256 amountIn, uint256) external returns (uint256) {
        return _exchange(amountIn, msg.sender);
    }

    function exchange(uint256, uint256, uint256 amountIn, uint256, bool) external returns (uint256) {
        return _exchange(amountIn, msg.sender);
    }

    function _exchange(uint256 amountIn, address receiver) private returns (uint256 amountOut) {
        amountOut = amountIn * 99 / 100;
        tokenIn.transferFrom(msg.sender, address(this), amountIn);
        tokenOut.transfer(receiver, amountOut);
    }
}
