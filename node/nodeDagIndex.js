'use strict';

const assert = require('assert');
const typeforce = require('typeforce');

const types = require('../types');

const debugLib = require('debug');

const debugNode = debugLib('node:app');
const debugBlock = debugLib('node:block');
const debugMsg = debugLib('node:messages');
const debugMsgFull = debugLib('node:messages:full');

module.exports = (Node, factory) => {
    const {
        Contract,
        Transport,
        Messages,
        Constants,
        Peer,
        PeerManager,
        Storage,
        Crypto,
        Mempool,
        Inventory,
        RPC,
        Application,
        Transaction,
        Block,
        PatchDB,
        Coins,
        PendingBlocksManager,
        MainDagIndex,
        BlockInfo,
        Mutex,
        RequestCache,
        TxReceipt,
        UTXO
    } = factory;
    const {MsgCommon, MsgVersion, PeerInfo, MsgAddr, MsgReject, MsgTx, MsgBlock, MsgInv, MsgGetData, MsgGetBlocks} =
        Messages;

    return class NodeDagIndex extends Node {
        /**
         * Return Set of hashes that are descendants of arrHashes
         * Overrides in-memory implementation of DAG with index
         *
         * @param {Array<String>} arrHashes - last known hashes
         * @returns {Set<any>} set of hashes descendants of arrHashes
         * @private
         */
        async _getBlocksFromLastKnown(arrHashes) {
            const setBlocksToSend = new Set();
            const mapKnownHashes = new Map();

            for (const strHash of arrHashes) {
                const nBlockHeight = await this._mainDagIndex.getBlockHeight(strHash);
                if (nBlockHeight !== null) {
                    mapKnownHashes.set(strHash, nBlockHeight);
                }
            }

            if (!mapKnownHashes.size) {
                // we missed at least one of those hashes! so we think peer is at wrong DAG
                // sent our version of DAG starting from Genesis

                // check do we have GENESIS self?
                const nBlockHeight = await this._mainDagIndex.getBlockHeight(Constants.GENESIS_BLOCK);

                if (nBlockHeight !== null) {
                    mapKnownHashes.set(Constants.GENESIS_BLOCK, nBlockHeight);

                    // Genesis wouldn't be included (same as all of arrHashes), so add it here
                    setBlocksToSend.add(Constants.GENESIS_BLOCK);
                } else {
                    // no GENESIS - return empty Set
                    return new Set();
                }
            }

            let mapCurrentLevel = new Map(mapKnownHashes);

            do {
                const mapNextLevel = new Map();
                for (const strHash of mapCurrentLevel.keys()) {
                    const objChildren = await this._mainDagIndex.getChildren(strHash, mapCurrentLevel.get(strHash));
                    for (const strChildHash in objChildren) {
                        // mainDagIndex has only direct children, so we don't need to check it
                        if (!mapKnownHashes.has(strChildHash) && !setBlocksToSend.has(strChildHash)) {
                            mapNextLevel.set(strChildHash, objChildren[strChildHash]);
                        }
                    }

                    if (!mapKnownHashes.has(strHash) && !setBlocksToSend.has(strHash)) {
                        setBlocksToSend.add(strHash);
                        if (setBlocksToSend.size > Constants.MAX_BLOCKS_INV) break;
                    }
                }
                mapCurrentLevel = new Map(mapNextLevel);
            } while (mapCurrentLevel.size && setBlocksToSend.size < Constants.MAX_BLOCKS_INV);

            return setBlocksToSend;
        }
    };
};
