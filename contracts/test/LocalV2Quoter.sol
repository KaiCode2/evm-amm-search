// SPDX-License-Identifier: MIT OR Apache-2.0
pragma solidity ^0.8.23;

/// @notice Deterministic quote target for the local container recovery gate.
/// @dev Production profiles must use their chain's deployed V2 router.
contract LocalV2Quoter {
    function getAmountsOut(uint256 amountIn, address[] calldata path) external pure returns (uint256[] memory amounts) {
        require(path.length >= 2, "short path");
        amounts = new uint256[](path.length);
        amounts[0] = amountIn;
        for (uint256 i = 1; i < path.length; i++) {
            amounts[i] = amounts[i - 1] * 997 / 1000;
        }
    }
}
