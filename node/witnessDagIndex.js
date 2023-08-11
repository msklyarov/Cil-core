'use strict';

const assert = require('assert');
const debugLib = require('debug');
const debugWitness = debugLib('witness:app');

module.exports = (Witness, factory) => {
    const {Node, Messages, Constants, BFT, Block, Transaction, BaseConciliumDefinition, PatchDB, BlockInfo} = factory;
    const {MsgWitnessCommon, MsgWitnessBlock, MsgWitnessWitnessExpose} = Messages;

    return class WitnessDagIndex extends Witness {

        _setConsensusHandlers(consensus) {
            consensus.on('message', message => {
                debugWitness(`Witness: "${this._debugAddress}" message "${message.message}" from CONSENSUS engine`);
                this._broadcastConsensusInitiatedMessage(message);
            });

            consensus.on('createBlock', async () => {
                if (this._mutex.isLocked('commitBlock') || this._isInitialBlockLoading()) return;

                const lock = await this._mutex.acquire(['createBlock', 'blockExec']);

                try {
                    const {conciliumId} = consensus;
                    const {block, patch} = await this._createBlock(conciliumId);
                    if (block.isEmpty() &&
                        (!consensus.timeForWitnessBlock() ||
                         this._isBigTimeDiff(block) ||
                         !this._pendingBlocks.isReasonToWitness(block)
                        )
                    ) {
                        this._suppressedBlockHandler();
                    } else {
                        await this._broadcastBlock(conciliumId, block);
                        consensus.processValidBlock(block, patch);
                    }
                } catch (e) {
                    logger.error(e);
                } finally {
                    this._mutex.release(lock);
                }
            });

            consensus.on('commitBlock', async (block, patch) => {
                const lock = await this._mutex.acquire(['commitBlock']);
                let lockBlock;

                try {
                    if (await this._isBlockKnown(block.hash())) {
                        throw new Error(`"commitBlock": block ${block.hash()} already known!`);
                    }

                    const arrContracts = [...patch.getContracts()];
                    if (arrContracts.length) {

                        // we have contracts inside block - we should re-execute block to have proper variables inside block
                        await this._handleArrivedBlock(block);
                    } else if (!this._mutex.isLocked('blockReceived') && !await this._isBlockExecuted(block.getHash())) {
                        lockBlock = await this._mutex.acquire(['blockReceived', block.getHash()]);

                        // block still hadn't received from more quick (that already commited & announced block) witness
                        // we have only moneys transfers, so we could use patch. this will speed up processing
                        if (!await this._isBlockExecuted(block.getHash())) {
                            await this._storeBlockAndInfo(block, new BlockInfo(block.header));
                            await this._acceptBlock(block, patch);
                            await this._postAcceptBlock(block);
                        }

                        if (!this._networkSuspended) this._informNeighbors(block);
                    }
                    logger.log(
                        `Witness: "${this._debugAddress}" block "${block.hash()}" Round: ${consensus.getCurrentRound()} commited at ${new Date} `);
                    consensus.blockCommited();

                } catch (e) {
                    logger.error(e);
                } finally {
                    this._mutex.release(lock);
                    if (lockBlock) this._mutex.release(lockBlock);
                }
            });
        }

        /**
         *
         * @param {Number} conciliumId - for which conciliumId we create block
         * @returns {Promise<{block, patch}>}
         * @private
         */
        async _createBlock(conciliumId) {
            const nStartTime = Date.now();
            const block = new Block(conciliumId);
            block.markAsBuilding();

            let arrParents;
            let patchMerged;

            try {
                ({arrParents, patchMerged} = await this._pendingBlocks.getBestParents(conciliumId));
                patchMerged = patchMerged ? patchMerged : new PatchDB();
                patchMerged.setConciliumId(conciliumId);

                assert(Array.isArray(arrParents) && arrParents.length, 'Couldn\'t get parents for block!');
                block.parentHashes = arrParents;
                block.setHeight(await this._calcHeight(arrParents));

                // variables for contracts (dummies)
                this._processedBlock = block;

                const arrBadHashes = [];
                let totalFee = 0;

                let arrTxToProcess;
                const arrUtxos = await this._storage.walletListUnspent(this._wallet.address);

                // There is possible situation with 1 UTXO having numerous output. It will be count as 1
                if (this._bCreateJoinTx && this._nLowestConciliumId === conciliumId && arrUtxos.length >
                    Constants.WITNESS_UTXOS_JOIN) {
                    arrTxToProcess = [
                        this._createJoinTx(arrUtxos, conciliumId, Constants.MAX_UTXO_PER_TX / 2),
                        ...this._gatherTxns(conciliumId)
                    ];
                } else {
                    arrTxToProcess = this._gatherTxns(conciliumId);
                }


                for (let tx of arrTxToProcess) {
                    try {

                        // with current timers and diameter if concilium more than 10 -
                        // TXns with 1000+ inputs will freeze network.
                        // So we'll skip this TXns
                        if (tx.inputs.length > Constants.MAX_UTXO_PER_TX) continue;
                        const {fee, patchThisTx} = await this._processTx(patchMerged, false, tx);

                        totalFee += fee;
                        patchMerged = patchMerged.merge(patchThisTx, true);
                        block.addTx(tx);

                        // this tx exceeded time limit for block creations - so we don't include it
                        if (Date.now() - nStartTime > Constants.BLOCK_CREATION_TIME_LIMIT) break;
                    } catch (e) {
                        logger.error(e);
                        arrBadHashes.push(tx.hash());
                    }
                }

                // remove failed txns
                if (arrBadHashes.length) this._mempool.removeTxns(arrBadHashes);

                block.finish(totalFee, this._wallet.address, await this._getFeeSizePerInput(conciliumId));
                await this._processBlockCoinbaseTX(block, totalFee, patchMerged);

                debugWitness(
                    `Witness: "${this._debugAddress}". Block ${block.hash()} with ${block.txns.length - 1} TXNs ready`);
            } catch (e) {
                logger.error(`Failed to create block!`, e);
            } finally {
                this._processedBlock = undefined;
            }

            return {block, patch: patchMerged};
        }
    }
}
