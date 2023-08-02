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

        /**
         * Build DAG of all known blocks! The rest of blocks will be added upon processing INV requests
         *
         * Because we need for _getBlocksFromLastKnown() only blocks with:
         * parentHashes.getHeight() - blockHash().getHeight() === 1
         * we could skip all others for the index
         *
         * @param {Array} arrLastStableHashes - hashes of all stable blocks
         * @param {Array} arrPedingBlocksHashes - hashes of all pending blocks
         */
        async _buildMainDagIndex(arrLastStableHashes, arrPedingBlocksHashes) {
            this._mainDagIndex = new MainDagIndex({storage: this._storage});
            const setProcessedHashes = new Set();

            // if we have only one concilium - all blocks becomes stable, and no pending!
            // so we need to start from stables
            let arrCurrentLevel =
                arrPedingBlocksHashes && arrPedingBlocksHashes.length ? arrPedingBlocksHashes : arrLastStableHashes;

            while (arrCurrentLevel.length) {
                const setNextLevel = new Set();
                for (let hash of arrCurrentLevel) {
                    debugNode(`Added ${hash} into dag`);

                    // we already processed this block
                    if (setProcessedHashes.has(hash)) continue;

                    const bi = await this._storage.getBlockInfo(hash);
                    if (!bi) throw new Error('_buildMainDag: Found missed blocks!');
                    if (bi.isBad()) throw new Error(`_buildMainDag: found bad block ${hash} in final DAG!`);

                    // add only if we have height distance === 1
                    this._mainDagIndex.addBlock(bi);

                    for (let parentHash of bi.parentHashes) {
                        if ((await this._mainDagIndex.getBlockHeight(parentHash)) === null) {
                            setNextLevel.add(parentHash);
                        }
                    }

                    setProcessedHashes.add(hash);
                }

                // Do we reach GENESIS?
                if (arrCurrentLevel.length === 1 && arrCurrentLevel[0] === Constants.GENESIS_BLOCK) break;

                // not yet
                arrCurrentLevel = [...setNextLevel.values()];
            }
        }

        /**
         * Used at startup to rebuild DAG of pending blocks
         *
         * @param {Array} arrLastStableHashes - hashes of LAST stable blocks
         * @param {Array} arrPendingBlocksHashes - hashes of all pending blocks
         * @returns {Promise<void>}
         */
        async _rebuildPending(arrLastStableHashes, arrPendingBlocksHashes) {
            const setStable = new Set(arrLastStableHashes);
            this._pendingBlocks = new PendingBlocksManager({
                mutex: this._mutex,
                arrTopStable: arrLastStableHashes
            });

            const mapBlocks = new Map();
            const setPatches = new Set();
            for (let hash of arrPendingBlocksHashes) {

                // Somtimes we have hash in both: pending & stable blocks (unexpected shutdown)?
                if (setStable.has(hash)) continue;

                hash = hash.toString('hex');
                const bi = await this._storage.getBlockInfo(hash);
                if (!bi) throw new Error('rebuildPending. Found missed blocks!');
                if (bi.isBad()) throw new Error(`rebuildPending: found bad block ${hash} in DAG!`);
                mapBlocks.set(hash, await this._storage.getBlock(hash));
            }

            const runBlock = async (hash) => {

                // are we already executed this block
                if (!mapBlocks.get(hash) || setPatches.has(hash)) return;

                const block = mapBlocks.get(hash);
                for (let parent of block.parentHashes) {
                    if (!setPatches.has(parent)) await runBlock(parent);
                }
                this._processedBlock = block;
                const patchBlock = await this._execBlock(block);

                await this._pendingBlocks.addBlock(block, patchBlock);

                setPatches.add(hash);
                this._processedBlock = undefined;
            };

            for (let hash of arrPendingBlocksHashes) {
                await runBlock(hash);
            }

            if (mapBlocks.size !== setPatches.size) throw new Error('rebuildPending. Failed to process all blocks!');
        }

        async _rebuildBlockDb() {
            await this._storage.ready();

            const nRebuildStarted = Date.now();

            const arrPendingBlocksHashes = await this._storage.getPendingBlockHashes();
            const arrLastStableHashes = await this._storage.getLastAppliedBlockHashes();

            await this._buildMainDagIndex(arrLastStableHashes, arrPendingBlocksHashes);
            await this._rebuildPending(arrLastStableHashes, arrPendingBlocksHashes);

            debugNode(`Rebuild took ${Date.now() - nRebuildStarted} msec.`);

            this._mempool.loadLocalTxnsFromDisk();
            await this._ensureLocalTxnsPatch();
        }
    };
};
