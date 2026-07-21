// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

import {ExperimentalExecutorRouter} from "./ExperimentalExecutorRouter.sol";

/// @notice EXPERIMENTAL CREATE2 factory for shared demo-router deployments.
/// @dev ABSOLUTELY NOT INTENDED FOR PUBLIC OR PRODUCTION USE. A deployment is
/// deterministic only for the same chain, factory address, salt, WETH, Permit2,
/// compiler settings, and router creation bytecode.
contract ExperimentalExecutorRouterFactory {
    event RouterDeployed(address indexed router, bytes32 indexed salt, address indexed weth, address permit2);

    function deploy(bytes32 salt, address weth, address permit2) external returns (address router) {
        router = computeAddress(salt, weth, permit2);
        if (router.code.length != 0) return router;
        router = address(new ExperimentalExecutorRouter{salt: salt}(weth, permit2));
        emit RouterDeployed(router, salt, weth, permit2);
    }

    function computeAddress(bytes32 salt, address weth, address permit2) public view returns (address router) {
        bytes32 initCodeHash =
            keccak256(abi.encodePacked(type(ExperimentalExecutorRouter).creationCode, abi.encode(weth, permit2)));
        router = address(uint160(uint256(keccak256(abi.encodePacked(bytes1(0xff), address(this), salt, initCodeHash)))));
    }
}
