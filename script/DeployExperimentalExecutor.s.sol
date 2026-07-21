// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

import {ExperimentalExecutorRouter} from "../contracts/ExperimentalExecutorRouter.sol";
import {ExperimentalExecutorRouterFactory} from "../contracts/ExperimentalExecutorRouterFactory.sol";

interface VmDeploy {
    function envAddress(string calldata name) external view returns (address value);
    function envBytes32(string calldata name) external view returns (bytes32 value);
    function envOr(string calldata name, address defaultValue) external view returns (address value);
    function envUint(string calldata name) external view returns (uint256 value);
    function startBroadcast(uint256 privateKey) external;
    function stopBroadcast() external;
}

/// @notice Deterministically deploy the executor through a new or existing factory.
/// @dev The emitted runtimeCodeHash is the exact value required by the sidecar.
contract DeployExperimentalExecutor {
    VmDeploy private constant vm = VmDeploy(address(uint160(uint256(keccak256("hevm cheat code")))));

    event ExecutorDeployment(
        address indexed factory,
        address indexed router,
        bytes32 indexed salt,
        address weth,
        address permit2,
        bytes32 runtimeCodeHash
    );

    function run() external returns (address factoryAddress, address routerAddress, bytes32 runtimeCodeHash) {
        uint256 deployerPrivateKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address weth = vm.envAddress("WETH_ADDRESS");
        address permit2 = vm.envAddress("PERMIT2_ADDRESS");
        bytes32 salt = vm.envBytes32("EXECUTOR_SALT");
        factoryAddress = vm.envOr("EXECUTOR_FACTORY", address(0));

        vm.startBroadcast(deployerPrivateKey);
        if (factoryAddress == address(0)) {
            factoryAddress = address(new ExperimentalExecutorRouterFactory());
        }
        routerAddress = ExperimentalExecutorRouterFactory(factoryAddress).deploy(salt, weth, permit2);
        vm.stopBroadcast();

        ExperimentalExecutorRouter router = ExperimentalExecutorRouter(routerAddress);
        require(router.WETH() == weth, "deployed router WETH mismatch");
        require(router.PERMIT2() == permit2, "deployed router Permit2 mismatch");
        runtimeCodeHash = routerAddress.codehash;
        emit ExecutorDeployment(factoryAddress, routerAddress, salt, weth, permit2, runtimeCodeHash);
    }
}
