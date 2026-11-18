// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// Emits one Ping per call, so a test can put known events in known blocks.
/// abis/Emitter.json is its ABI and bytecode from `forge build --use 0.8.28 --optimize --no-metadata`.
contract Emitter {
    event Ping(address indexed sender, uint256 indexed round, uint256 value);

    function ping(uint256 round, uint256 value) external {
        emit Ping(msg.sender, round, value);
    }
}
