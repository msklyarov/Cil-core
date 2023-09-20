'use strict';

const assert = require('assert');
const typeforce = require('typeforce');

const types = require('../types');

const debugLib = require('debug');

const debugNode = debugLib('node:app');
const debugBlock = debugLib('node:block');
const debugMsg = debugLib('node:messages');
const debugMsgFull = debugLib('node:messages:full');

function createPeerKey(peer) {
    return peer.address + peer.port;
}

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
    const {MSG_VERSION, MSG_VERACK, MSG_GET_ADDR, MSG_ADDR, MSG_REJECT, MSG_GET_MEMPOOL} = Constants.messageTypes;

    return class NodeDagIndex extends Node {
        /**
         * Handler for MSG_INV message
         * Send MSG_GET_DATA for unknown hashes
         *
         * @param {Peer} peer - peer that send message
         * @param {MessageCommon} message
         * @return {Promise<void>}
         * @private
         */
        async _handleInvMessage(peer, message) {
            const invMsg = new MsgInv(message);
            const invToRequest = new Inventory();

            const lock = await this._mutex.acquire(['inventory']);
            try {
                let nBlockToRequest = 0;
                for (let objVector of invMsg.inventory.vector) {
                    // we already requested it (from another peer), so let's skip it
                    if (this._requestCache.isRequested(objVector.hash)) continue;

                    let bShouldRequest = false;
                    if (objVector.type === Constants.INV_TX) {
                        bShouldRequest = !this._mempool.hasTx(objVector.hash) && !this._isInitialBlockLoading();
                        if (bShouldRequest) {
                            try {
                                await this._storage.getUtxo(objVector.hash, true).catch();
                                bShouldRequest = false;
                            } catch (e) {}
                        }
                    } else if (objVector.type === Constants.INV_BLOCK) {
                        const strHash = objVector.hash.toString('hex');
                        // const bBlockKnown=await this._isBlockKnown(strHash);
                        const objBlockInfo = await this._storage.getBlockInfo(strHash).catch(() => null);

                        bShouldRequest =
                            !this._storage.isBlockBanned(strHash) &&
                            !this._requestCache.isRequested(strHash) &&
                            !objBlockInfo;
                        if (bShouldRequest) nBlockToRequest++;

                        // i.e. we store it, it somehow missed dag
                        if (objBlockInfo && !(await this._mainDagIndex.has(strHash, objBlockInfo.getHeight()))) {
                            await this._processStoredBlock(strHash, peer);
                        }
                    }

                    if (bShouldRequest) {
                        invToRequest.addVector(objVector);
                        this._requestCache.request(objVector.hash);
                        debugMsgFull(`Will request "${objVector.hash.toString('hex')}" from "${peer.address}"`);
                    }
                }

                // inventory could contain TXns
                if (invToRequest.vector.length) {
                    const msgGetData = new MsgGetData();
                    msgGetData.inventory = invToRequest;
                    debugMsg(
                        `(address: "${this._debugAddress}") requesting ${invToRequest.vector.length} hashes from "${peer.address}"`
                    );
                    await peer.pushMessage(msgGetData);
                }

                // was it reponse to MSG_GET_BLOCKS ?
                if (peer.isGetBlocksSent()) {
                    if (nBlockToRequest > 1) {
                        // so we should resend MSG_GET_BLOCKS later
                        peer.markAsPossiblyAhead();
                    } else {
                        peer.markAsEven();

                        if (nBlockToRequest === 1) {
                            peer.singleBlockRequested();
                        } else if (!this._isInitialBlockLoading()) {
                            // we requested blocks from equal peer and receive NOTHING new, now we can request his mempool
                            const msgGetMempool = new MsgCommon();
                            msgGetMempool.getMempoolMessage = true;
                            debugMsg(
                                `(address: "${this._debugAddress}") sending "${MSG_GET_MEMPOOL}" to "${peer.address}"`
                            );
                            await peer.pushMessage(msgGetMempool);
                        }
                    }
                    peer.doneGetBlocks();
                }
            } catch (e) {
                throw e;
            } finally {
                this._mutex.release(lock);
            }
        }

        /**
         * Handler for MSG_GET_BLOCKS message.
         * Send MSG_INV for further blocks (if we have it)
         *
         * @param {Peer} peer - peer that send message
         * @param {MessageCommon} message - it contains hashes of LAST FINAL blocks!
         * @return {Promise<void>}
         * @private
         */
        async _handleGetBlocksMessage(peer, message) {

            // we'r empty. we have nothing to share with party
            if (!await this._mainDagIndex.getOrder()) return;

            const msg = new MsgGetBlocks(message);
            const inventory = new Inventory();

            for (let hash of await this._getBlocksFromLastKnown(msg.arrHashes)) {
                inventory.addBlockHash(hash);
            }
            debugMsg(
                `(address: "${this._debugAddress}") sending ${inventory.vector.length} blocks to "${peer.address}"`);

            const msgInv = new MsgInv();
            msgInv.inventory = inventory;
            debugMsg(`(address: "${this._debugAddress}") sending "${msgInv.message}" to "${peer.address}"`);
            await peer.pushMessage(msgInv);
        }

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
         * Handler for 'addr message
         *
         * @param {Peer} peer - peer that send message
         * @param {MessageCommon} message
         * @return {Promise<void>}
         * @private
         */
        async _handlePeerList(peer, message) {
            message = new MsgAddr(message);
            for (let peerInfo of message.peers) {

                // don't add own address
                if (this._myPeerInfo.address.equals(PeerInfo.toAddress(peerInfo.address))) continue;

                const newPeer = await this._peerManager.addPeer(peerInfo, false);
                if (newPeer instanceof Peer) {
                    debugNode(`(address: "${this._debugAddress}") added peer "${newPeer.address}" to peerManager`);
                }
            }

            // next stage: request unknown blocks or just GENESIS, if we are at very beginning
            let msg;
            if (Constants.GENESIS_BLOCK && !await this._mainDagIndex.has(Constants.GENESIS_BLOCK)) {
                msg = this._createGetDataMsg([Constants.GENESIS_BLOCK]);
                peer.markAsPossiblyAhead();
            } else {
                msg = await this._createGetBlocksMsg();
                debugMsg(`(address: "${this._debugAddress}") sending "${msg.message}" to "${peer.address}"`);
            }
            await peer.pushMessage(msg);

            // TODO: move loadDone after we got all we need from peer
            peer.loadDone = true;
        }

        async _createGetBlocksMsg() {
            const msg = new MsgGetBlocks();
            const arrLastApplied = await this._storage.getLastAppliedBlockHashes();
            const objBlockHeights = {};
            await Promise.all(
                arrLastApplied.map(async strHash => {
                    objBlockHeights[strHash] = await this._mainDagIndex.getBlockHeight(strHash);
                })
            );

            arrLastApplied.sort(
                (hashA, hashB) => objBlockHeights[hashB] - objBlockHeights[hashA]
            );
            const arrTips = this._pendingBlocks.getTips();
            msg.arrHashes = arrTips.length ? arrTips.concat([arrLastApplied[0]]) : arrLastApplied;
            return msg;
        }

        /**
         *
         * @param {String} event - event name
         * @param {*} content
         * @return {Promise<void>}
         * @private
         */
        async rpcHandler({event, content}) {

            try {
                switch (event) {
                    case 'getLastBlockByConciliumId':
                        return await this.getLastBlockByConciliumId(content);
                    case 'tx':
                        return await this._acceptLocalTx(content);
                    case 'getContractData':
                        return await this._getContractData(content);
                    case 'txReceipt':
                        return await this._getTxReceipt(content);
                    case 'getBlock':

                        // content is hash
                        return await this._getBlockAndState(content).catch(err => debugNode(err));
                    case 'getTips': {
                        let arrHashes = this._pendingBlocks.getTips();

                        if (!arrHashes || !arrHashes.length) {
                            arrHashes = await this._storage.getLastAppliedBlockHashes();
                        }
                        if (!arrHashes) return [];

                        return await Promise.all(
                            arrHashes.map(async h => await this._getBlockAndState(h).catch(err => debugNode(err)))
                        );
                    }
                    case 'getNext': {
                        // only for children with height difference === 1
                        const objBlockInfo = await this._storage.getBlockInfo(content).catch(() => ({}));
                        let arrChildHashes = Object.keys(this._mainDagIndex.getChildren(content, objBlockInfo.getHeight()));
                        if (!arrChildHashes || !arrChildHashes.length) {
                            arrChildHashes = this._pendingBlocks.getChildren(content);
                        }
                        if (!arrChildHashes) return [];
                        return await Promise.all(
                            arrChildHashes.map(async h => await this._getBlockAndState(h).catch(err => debugNode(err)))
                        );
                    }
                    case 'getPrev': {
                        let cBlockInfo = await this._mainDagIndex.getBlockInfo(content);
                        if (!cBlockInfo) {
                            cBlockInfo = this._pendingBlocks.getBlock(content).blockHeader;
                        }
                        if (!cBlockInfo) return [];
                        return await Promise.all(
                            cBlockInfo.parentHashes.map(
                                async h => await this._getBlockAndState(h.toString('hex')).catch(err => debugNode(err)))
                        );
                    }
                    case 'getTx':
                        return await this._getTxForRpc(content);
                    case 'constantMethodCall':
                        return await this._constantMethodCallRpc(content);
                    case 'getUnspent':
                        const utxo = await this._storage.getUtxo(content);
                        return utxo.toObject();
                    case 'getWitnesses':
                        return await this._getAllWitnesses();
                    case 'getConnectedPeers':
                        return this._peerManager.getConnectedPeers();
                    case 'getBannedPeers':
                        return this._peerManager.getBannedPeers();
                    case 'getMempoolContent':
                        return this._mempool.getContent();
                    default:
                        throw new Error(`Unsupported method ${event}`);
                }
            } catch (e) {
                logger.error('RPC error.', e);
                throw e;
            }
        }

        /**
         * Process block:
         * - verify
         * - run Application for each tx
         * - return patch (or null) that could be applied to storage
         *
         * @param {Block} block
         * @returns {PatchDB | null}
         * @private
         */
        async _execBlock(block) {
            const isGenesis = this.isGenesisBlock(block);

            // double check: whether we already processed this block?
            if (await this._isBlockExecuted(block.getHash())) {
                debugNode(`Trying to process ${block.getHash()} more than one time!`);
                return null;
            }

            // check for correct block height
            if (!isGenesis) await this._checkHeight(block);

            let patchState = await this._pendingBlocks.mergePatches(block.parentHashes);
            patchState.setConciliumId(block.conciliumId);

            let blockFees = 0;
            const blockTxns = block.txns;

            // should start from 1, because coinbase tx need different processing
            for (let i = 1; i < blockTxns.length; i++) {
                const tx = new Transaction(blockTxns[i]);
                assert(tx.conciliumId === block.conciliumId, `Tx ${tx.getHash()} conciliumId differ from block's one`);
                const {fee, patchThisTx} = await this._processTx(patchState, isGenesis, tx);
                blockFees += fee;
                patchState = patchState.merge(patchThisTx, true);
            }

            // process coinbase tx
            if (!isGenesis) {
                await this._processBlockCoinbaseTX(block, blockFees, patchState);
            }

            debugNode(`Block ${block.getHash()} being executed`);
            return patchState;
        }

        async _processFinalityResults(result) {
            if (!result) return;
            const {
                patchToApply,
                setStableBlocks,
                setBlocksToRollback,
                arrTopStable
            } = result;

            logger.log(`Blocks ${Array.from(setStableBlocks.keys())} are stable now`);

            await this._updateLastAppliedBlocks(arrTopStable);

            let nHeightMax = 0;
            for (let hash of setStableBlocks) {
                // const bi = this._mainDag.getBlockInfo(hash);
                const bi = await this._storage.getBlockInfo(hash);
                if (bi.getHeight() > nHeightMax) nHeightMax = bi.getHeight();
                bi.markAsFinal();
                // this._mainDag.setBlockInfo(bi);
                await this._storage.saveBlockInfo(bi);
            }

            await this._storage.applyPatch(patchToApply, nHeightMax);

            // revalidate local TXns. it affects only local TXns, so it doesn't duplicate
            // validation in _unwindBlock
            this._patchLocalTxns = undefined;
            await this._ensureLocalTxnsPatch();

            for (let blockHash of setBlocksToRollback) {
                await this._unwindBlock(await this._storage.getBlock(blockHash));
            }
            await this._storage.removeBadBlocks(setBlocksToRollback);

            if (this._rpc) {
                this._rpc.informWsSubscribersStableBlocks(Array.from(setStableBlocks.keys()));
            }
        }

        async _updateLastAppliedBlocks(arrTopStable) {
            const arrPrevTopStableBlocks = await this._storage.getLastAppliedBlockHashes();
            const mapPrevConciliumIdHash = new Map();
            for (const hash of arrPrevTopStableBlocks) {
                const cBlockInfo = await this._storage.getBlockInfo(hash);
                mapPrevConciliumIdHash.set(cBlockInfo.getConciliumId(), hash);
            }

            const mapNewConciliumIdHash = new Map();
            for (const hash of arrTopStable) {
                const cBlockInfo = await this._storage.getBlockInfo(hash);
                mapNewConciliumIdHash.set(cBlockInfo.getConciliumId(), hash);
            }

            const arrNewLastApplied = [];

            const nConciliumCount = await this._storage.getConciliumsCount();
            for (let i = 0; i <= nConciliumCount; i++) {
                const hash = mapNewConciliumIdHash.get(i) || mapPrevConciliumIdHash.get(i);

                // concilium could be created, but still no final blocks
                if (hash) arrNewLastApplied.push(hash);
            }

            await this._storage.updateLastAppliedBlocks(arrNewLastApplied);

            this._createPseudoRandomSeed(arrNewLastApplied);
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
            // const setProcessedHashes = new Set();

            // if we have only one concilium - all blocks becomes stable, and no pending!
            // so we need to start from stables
            let arrCurrentLevel =
                arrPedingBlocksHashes && arrPedingBlocksHashes.length ? arrPedingBlocksHashes : arrLastStableHashes;

            while (arrCurrentLevel.length) {
                const setNextLevel = new Set();
                for (let hash of arrCurrentLevel) {
                    debugNode(`Added ${hash} into dag`);

                    // we already processed this block
                    // if (setProcessedHashes.has(hash)) continue;
                    if (this._mainDagIndex.getBlockInfo(hash)) continue;

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

                    // setProcessedHashes.add(hash);
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

            const runBlock = async hash => {
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

        /**
         * Check was parents executed?
         *
         * @param {Block | BlockInfo} block
         * @return {Promise<boolean || Set>}
         * @private
         */
        async _canExecuteBlock(block) {
            if (this.isGenesisBlock(block)) return true;

            for (let hash of block.parentHashes) {
                let blockInfo = await this._mainDagIndex.getBlockInfo(hash);

                // parent is bad
                if (blockInfo && blockInfo.isBad()) {
                    throw new Error(`Block ${block.getHash()} refer to bad parent ${hash}`);
                }

                // parent is good!
                if ((blockInfo && blockInfo.isFinal()) || this._pendingBlocks.hasBlock(hash)) continue;

                return false;
            }
            return true;
        }

        /**
         * Block failed to become FINAL, let's unwind it
         *
         * @param {Block} block
         * @private
         */
        async _unwindBlock(block) {
            logger.log(`(address: "${this._debugAddress}") Unwinding txns from block: "${block.getHash()}"`);

            // skip coinbase
            for (let i = 1; i < block.txns.length; i++) {
                await this._processReceivedTx(new Transaction(block.txns[i]), true).catch(err => {});
            }

            try {
                await this._pendingBlocks.removeBlock(block.getHash());
                await this._mainDagIndex.removeBlock(block);
            } catch (e) {}
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

        /**
         * Main worker that will be restarted periodically
         *
         * _mapBlocksToExec is map of hash => peer (that sent us a block)
         * @returns {Promise<void>}
         * @private
         */
        async _blockProcessor() {
            if (this._isBusyWithExec()) return;

            if (this._mapBlocksToExec.size) {
                debugBlock(`Block processor started. ${this._mapBlocksToExec.size} blocks awaiting to exec`);

                for (let [hash, peer] of this._mapBlocksToExec) {
                    // we have no block in DAG, but possibly have it in storage
                    const blockOrInfo = await this._storage.getBlock(hash).catch(err => debugBlock(err));
                    if (blockOrInfo) await this._blockInFlight(blockOrInfo, true);

                    try {
                        if (!blockOrInfo || (blockOrInfo.isBad && blockOrInfo.isBad())) {
                            throw new Error(`Block ${hash} is not found or bad`);
                        }
                        await this._processBlock(blockOrInfo, peer);
                    } catch (e) {
                        logger.error(e);
                        if (blockOrInfo) await this._blockBad(blockOrInfo);
                    } finally {
                        debugBlock(`Removing block ${hash} from BlocksToExec`);
                        this._mapBlocksToExec.delete(hash);
                    }
                }
            } else if (this._requestCache.isEmpty()) {
                await this._queryPeerForRestOfBlocks();
            }

            if (this._mapUnknownBlocks.size) {
                await this._requestUnknownBlocks();
            }
        }

        /**
         *
         * @param {Block | BlockInfo} block
         * @param {Peer} peer
         * @returns {Promise<void>}
         * @private
         */
        async _processBlock(block, peer) {
            typeforce(typeforce.oneOf(types.Block, types.BlockInfo), block);

            debugBlock(`Attempting to exec block "${block.getHash()}"`);

            if (await this._canExecuteBlock(block)) {
                if (!await this._isBlockExecuted(block.getHash())) {
                    await this._blockProcessorExecBlock(block instanceof Block ? block : block.getHash(), peer);

                    const objChildrenHashes = await this._mainDagIndex.getChildren(block.getHash(), block.getHeight());
                    for (let hash in objChildrenHashes) {
                        await this._queueBlockExec(hash, peer);
                    }
                }
            } else {
                await this._queueBlockExec(block.getHash(), peer);
                const {arrToRequest, arrToExec} = await this._blockProcessorProcessParents(block);
                arrToRequest
                    .filter(hash => !this._storage.isBlockBanned(hash))
                    .forEach(hash => this._mapUnknownBlocks.set(hash, peer));

                for (const hash of arrToExec) {
                    await this._queueBlockExec(hash, peer);
                }
            }
        }

        async _queueBlockExec(hash, peer) {
            debugBlock(`Adding block ${hash} from BlocksToExec`);

            const blockInfo = await this._mainDagIndex.getBlockInfo(hash);
            if (blockInfo && blockInfo.isBad()) return;

            this._mapBlocksToExec.set(hash, peer);
        }

        async _blockProcessorProcessParents(blockInfo) {
            typeforce(typeforce.oneOf(types.Block, types.BlockInfo), blockInfo);

            const arrToRequest = [];
            const arrToExec = [];
            for (let parentHash of blockInfo.parentHashes) {

                // if we didn't queue it for exec & we don't have it yet
                if (!this._mapBlocksToExec.has(parentHash) && !await this._isBlockKnown(parentHash)) {
                    arrToRequest.push(parentHash);
                } else {
                    if (!await this._isBlockExecuted(parentHash)) {
                        arrToExec.push(parentHash);
                    }
                }
            }

            return {arrToRequest, arrToExec};
        }

        async _blockProcessorExecBlock(blockOrHash, peer) {
            typeforce(typeforce.oneOf(types.Hash256bit, types.Block), blockOrHash);

            const block = blockOrHash instanceof Block ? blockOrHash : await this._storage.getBlock(blockOrHash);

            debugBlock(`Executing block "${block.getHash()}"`);

            const lock = await this._mutex.acquire(['blockExec', block.getHash()]);
            this._processedBlock = block;
            try {
                const patchState = await this._execBlock(block);
                if (patchState && !await this._isBlockExecuted(block.getHash())) {
                    await this._acceptBlock(block, patchState);
                    await this._postAcceptBlock(block);
                    if (!this._networkSuspended && !this._isInitialBlockLoading()) this._informNeighbors(block, peer);
                }
            } catch (e) {
                logger.error(`Failed to execute "${block.hash()}"`, e);
                await this._blockBad(block);
                peer.misbehave(10);
            } finally {
                this._mutex.release(lock);
                this._processedBlock = undefined;
            }
        }

        async _isBlockExecuted(strHash) {
            const blockInfo = await await this._storage.getBlockInfo(strHash).catch(() => null);
            return (
                (blockInfo && blockInfo.isFinal() && (await this._mainDagIndex.has(strHash, blockInfo.getHeight()))) ||
                this._pendingBlocks.hasBlock(strHash)
            );
        }

        async _isBlockKnown(strHash) {
            return await this._storage.hasBlock(strHash);
        }

        /**
         * Depending of BlockInfo flag - store block & it's info in _mainDag & _storage
         *
         * @param {Block | undefined} block
         * @param {BlockInfo} blockInfo
         * @param {Boolean} bOnlyDag - store only in DAG
         * @private
         */
        async _storeBlockAndInfo(block, blockInfo, bOnlyDag) {
            typeforce(typeforce.tuple(typeforce.oneOf(types.Block, undefined), types.BlockInfo), arguments);

            await this._mainDagIndex.addBlock(blockInfo);
            if (bOnlyDag) return;

            if (blockInfo.isBad()) {
                const storedBI = await this._storage.getBlockInfo(blockInfo.getHash()).catch(err => debugNode(err));
                if (storedBI && !storedBI.isBad()) {
                    // rewrite it's blockInfo
                    await this._storage.saveBlockInfo(blockInfo);

                    // remove block (it was marked as good block)
                    await this._storage.removeBlock(blockInfo.getHash());
                } else {
                    // we don't store entire of bad blocks, but store its headers (to prevent processing it again)
                    await this._storage.saveBlockInfo(blockInfo);
                }
            } else {
                // save block, and it's info
                await this._storage.saveBlock(block, blockInfo).catch(err => debugNode(err));
            }
        }

        async _requestUnknownBlocks() {
            // request all unknown blocks
            const {mapPeerBlocks, mapPeerAhead} = await this._createMapBlockPeer();
            for (let peer of mapPeerAhead.values()) {
                await this._queryPeerForRestOfBlocks(peer);
            }
            await this._sendMsgGetDataToPeers(mapPeerBlocks);
        }

        /**
         * Which hashes of this._mapUnknownBlocks should be queried from which peer
         * Or if peer seems to be ahead of us - send MsgGetBlocks
         *
         * @returns {Map, Map} mapPeerBlocks: {peerKey => Set of hashes}, mapPeerAhead {peerKey => peer}
         * @private
         */
        async _createMapBlockPeer() {
            const mapPeerBlocks = new Map();
            const mapPeerAhead = new Map();

            for (let [hash, peer] of this._mapUnknownBlocks) {
                if (this._requestCache.isRequested(hash) || await this._mainDagIndex.has(hash)) continue;

                const key = createPeerKey(peer);

                // we'll batch request block from this peer
                if (peer.isAhead()) {
                    mapPeerAhead.set(key, peer);
                    continue;
                }

                let setBlocks = mapPeerBlocks.get(key);
                if (!setBlocks) {
                    setBlocks = new Set();
                    mapPeerBlocks.set(key, setBlocks);
                }
                setBlocks.add(hash);
            }
            return {mapPeerBlocks, mapPeerAhead};
        }

        async _getBlockAndState(hash) {
            typeforce(types.Str64, hash);

            const cBlock = await this._storage.getBlock(hash);
            const blockInfo = await this._storage.getBlockInfo(hash);

            return {block: cBlock, state: blockInfo ? blockInfo.getState() : undefined};
        }

        /**
         * Height is longest path in DAG
         *
         * @param {Array} arrParentHashes - of strHashes
         * @return {Number}
         * @private
         */
        async _calcHeight(arrParentHashes) {
            typeforce(typeforce.arrayOf(types.Hash256bit), arrParentHashes);

            const arrHeights = [];
            for (const strHash of arrParentHashes) {
                const blockInfo = await this._storage.getBlockInfo(strHash).catch(err => debugBlock(err));
                arrHeights.push(blockInfo.getHeight());
            }

            return Math.max(...arrHeights) + 1;
        }

        /**
         *
         * @param {Block} block
         * @private
         */
        async _checkHeight(block) {
            const calculatedHeight = await this._calcHeight(block.parentHashes);
            assert(calculatedHeight === block.getHeight(),
                `Incorrect height "${calculatedHeight}" were calculated for block ${block.getHash()} (expected ${block.getHeight()}`
            );
        }

        /**
         * Rebuild chainstate from blockDb (reExecute them one more time)
         *
         * @param {String | undefined} strHashToStop - hash of block to stop rebuild. this block also will be executed
         * @returns {Promise<void>}
         */
        async rebuildDb(strHashToStop) {
            // this._mainDag = new MainDag();
            this._mainDagIndex = new MainDagIndex({storage: this._storage});

            for await (let {value} of this._storage.readBlocks()) {
                const block = new factory.Block(value);
                // await this._mainDag.addBlock(new BlockInfo(block.header));
                await this._mainDagIndex.addBlock(new BlockInfo(block.header));
            }

            this._queryPeerForRestOfBlocks = this._requestUnknownBlocks = () => {
                logger.error('we have unresolved dependencies! will possibly fail to rebuild DB');
            };

            const originalQueueBlockExec = this._queueBlockExec.bind(this);
            let bStop = Constants.GENESIS_BLOCK === strHashToStop;
            this._queueBlockExec = async (hash, peer) => {
                if (bStop) return;
                if (hash === strHashToStop) bStop = true;
                // TODO: Check this part, but now we store BlockInfo only in storage
                // const blockInfo = this._mainDag.getBlockInfo(hash);
                // this._storage.saveBlockInfo(blockInfo).catch(err => logger.error(err));
                await originalQueueBlockExec(hash, peer);
            };

            // const genesis = this._mainDag.getBlockInfo(Constants.GENESIS_BLOCK);
            const genesis = await this._storage.getBlockInfo(Constants.GENESIS_BLOCK);
            assert(genesis, 'No Genesis found');
            this._mapBlocksToExec.set(genesis.getHash(), undefined);

            await this._blockProcessor();
        }

        async _processStoredBlock(strHash, peer) {
            const bi = await this._storage.getBlockInfo(strHash);
            await this._storeBlockAndInfo(undefined, bi, true);
            await this._queueBlockExec(strHash, peer);
        }
    };
};
