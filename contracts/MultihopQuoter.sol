// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract MultihopQuoter {
    uint8 internal constant DECODE_UINT256 = 0;
    uint8 internal constant DECODE_UINT256_ARRAY_LAST = 1;
    uint8 internal constant DECODE_FIRST_WORD = 2;

    struct Hop {
        address target;
        bytes data;
        uint256 amountOffset;
        uint8 decodeMode;
    }

    error BadCalldata(uint256 index);
    error BadOutput(uint256 index);
    error HopFailed(uint256 index, bytes reason);

    function quote(uint256 amountIn, Hop[] calldata hops) external returns (uint256 amountOut) {
        amountOut = amountIn;

        for (uint256 i = 0; i < hops.length; i++) {
            Hop calldata hop = hops[i];
            bytes memory data = hop.data;

            if (data.length < hop.amountOffset + 32) {
                revert BadCalldata(i);
            }

            uint256 amount = amountOut;
            uint256 offset = hop.amountOffset;
            assembly {
                mstore(add(add(data, 0x20), offset), amount)
            }

            (bool ok, bytes memory output) = hop.target.call(data);
            if (!ok) {
                revert HopFailed(i, output);
            }

            if (hop.decodeMode == DECODE_UINT256) {
                if (output.length < 32) {
                    revert BadOutput(i);
                }
                amountOut = abi.decode(output, (uint256));
            } else if (hop.decodeMode == DECODE_UINT256_ARRAY_LAST) {
                uint256[] memory amounts = abi.decode(output, (uint256[]));
                if (amounts.length == 0) {
                    revert BadOutput(i);
                }
                amountOut = amounts[amounts.length - 1];
            } else if (hop.decodeMode == DECODE_FIRST_WORD) {
                if (output.length < 32) {
                    revert BadOutput(i);
                }
                assembly {
                    amountOut := mload(add(output, 0x20))
                }
            } else {
                revert BadOutput(i);
            }
        }
    }
}
