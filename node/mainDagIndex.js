'use strict';

const assert = require('assert');
const typeforce = require('typeforce');
const types = require('../types');

module.exports = ({Constants, Crypto}) => {
    const {MAIN_DAG_INDEX_STEP} = Constants;
    return class MainDagIndex {
        constructor(props) {
            const {storage} = props;
            assert(storage, 'MainDagIndex constructor requires Storage instance!');

            this._storage = storage;
            this._strDagPrefix = Crypto.createHash(Date.now().toString());
            this._pagesCache = {}; // Store MAIN_DAG_PAGES_IN_MEMORY
        }

        async addBlock(blockInfo) {
            typeforce(types.BlockInfo, blockInfo);

            const nBlockHeight = blockInfo.getHeight();
            const strBlockHash = blockInfo.getHash();

            if (strBlockHash !== Constants.GENESIS_BLOCK) {
                // add parents

                for (const strParentBlockHash of blockInfo.parentHashes) {
                    const objParentBlock = await this._storage.getBlockInfo(strParentBlockHash).catch(() => null);
                    if (!objParentBlock) continue;

                    const nParentBlockHeight = objParentBlock.getHeight();

                    let arrParentHashes = await this._getMainDagPageIndex(nParentBlockHeight);

                    if (!arrParentHashes) {
                        arrParentHashes = {};
                    }

                    if (nBlockHeight - nParentBlockHeight === 1) {
                        const objIndex = arrParentHashes[strParentBlockHash];

                        if (!objIndex) {
                            arrParentHashes[strParentBlockHash] = [false, {[strBlockHash]: nBlockHeight}];
                            await this._storage.incMainDagIndexOrder(this._strDagPrefix);
                        } else {
                            arrParentHashes[strParentBlockHash] = [
                                objIndex[0],
                                {...objIndex[1], [strBlockHash]: nBlockHeight}
                            ];
                        }
                    }

                    await this._setMainDagPageIndex(nParentBlockHeight, arrParentHashes);
                }
            }

            // process block
            let arrHashes = await this._getMainDagPageIndex(nBlockHeight);
            if (!arrHashes) {
                arrHashes = [];
            }

            const objBlock = arrHashes[strBlockHash];
            if (!objBlock) {
                arrHashes[strBlockHash] = [true, {}];
                await this._storage.incMainDagIndexOrder(this._strDagPrefix);
            } else if (!objBlock[0]) {
                arrHashes[strBlockHash] = [true, objBlock[1]];
            }

            await this._setMainDagPageIndex(nBlockHeight, arrHashes);
        }

        async removeBlock(blockInfo) {
            typeforce(types.BlockInfo, blockInfo);

            const strHash = blockInfo.getHash();
            const nBlockHeight = blockInfo.getHeight();

            let arrHashes = await this._getMainDagPageIndex(nBlockHeight);
            if (!arrHashes) return;

            // если это единственный блок то тут порядок должен на 2 величины измениться
            if (arrHashes[strHash]) {
                delete arrHashes[strHash];
                await this._setMainDagPageIndex(nBlockHeight, arrHashes);
                await this._storage.decMainDagIndexOrder(this._strDagPrefix);
            }

            for (const strParentBlockHash of blockInfo.parentHashes) {
                const objParentBlock = await this._storage.getBlockInfo(strParentBlockHash).catch(() => null);
                if (!objParentBlock) continue;

                const nParentBlockHeight = objParentBlock.getHeight();

                const arrParentHashes = await this._getMainDagPageIndex(nParentBlockHeight);
                if (!arrParentHashes) continue;

                const [bIsProcessed, objChildren] = arrParentHashes[strParentBlockHash];

                if (objChildren[strHash]) {
                    delete objChildren[strHash];
                    if (Object.keys(objChildren).length === 0 && !bIsProcessed) {
                        delete arrParentHashes[strParentBlockHash];
                        await this._storage.decMainDagIndexOrder(this._strDagPrefix);
                    } else {
                        arrParentHashes[strParentBlockHash] = [bIsProcessed, objChildren];
                    }

                    await this._setMainDagPageIndex(nBlockHeight, arrParentHashes);
                }
            }
        }

        async getChildren(strHash, nBlockHeight) {
            typeforce(types.Str64, strHash);
            typeforce('Number', nBlockHeight);

            const indexPage = await this._getMainDagPageIndex(nBlockHeight);

            return indexPage && indexPage[strHash] && indexPage[strHash][0] ? indexPage[strHash][1] : {};
        }

        async getBlockHeight(strHash) {
            typeforce(types.Str64, strHash);

            const objBlockInfo = await this._storage.getBlockInfo(strHash).catch(() => null);
            if (!objBlockInfo) return null;

            const nBlockHeight = objBlockInfo.getHeight();

            return (await this.has(strHash, nBlockHeight)) ? nBlockHeight : null;
        }

        async has(strHash, nBlockHeight = undefined) {
            typeforce(types.Str64, strHash);
            typeforce(typeforce.oneOf('Number', undefined), nBlockHeight);

            let nHeight = nBlockHeight;
            if (nHeight === undefined) {
                const objBlockInfo = await this._storage.getBlockInfo(strHash).catch(() => null);
                if (!objBlockInfo) return false;
                nHeight = objBlockInfo.getHeight();
            }

            const indexPage = await this._getMainDagPageIndex(nHeight);

            return indexPage && indexPage[strHash] && indexPage[strHash][0];
        }

        async getBlockInfo(strHash) {
            typeforce(types.Str64, strHash);

            const objBlockInfo = await this._storage.getBlockInfo(strHash).catch(() => null);
            if (!objBlockInfo) return null;

            const indexPage = await this._getMainDagPageIndex(objBlockInfo.getHeight());

            return indexPage && indexPage[strHash] && indexPage[strHash][0] ? objBlockInfo : null;
        }

        async getOrder() {
            return await this._storage.getMainDagIndexOrder(this._strDagPrefix);
        }

        _getPageIndexByHeight(nHeight) {
            return Math.floor(nHeight / MAIN_DAG_INDEX_STEP) * (MAIN_DAG_INDEX_STEP - 1);
        }

        async _getMainDagPageIndex(nBlockHeight) {
            const nPageIndex = this._getPageIndexByHeight(nBlockHeight);

            const objPage = this._pagesCache[nPageIndex];

            if (objPage) {
                objPage.timestamp = Date.now();
                return objPage.data;
            }

            this._releaseOldCachePages();

            const pageData = await this._storage.getMainDagPageIndex(this._getDbRecordIndex(nPageIndex));

            if (!pageData) return null;

            // add to cache
            this._pagesCache[nPageIndex] = {
                timestamp: Date.now(),
                data: pageData
            };

            return pageData;
        }

        async _setMainDagPageIndex(nBlockHeight, arrHashes) {
            const nPageIndex = this._getPageIndexByHeight(nBlockHeight);

            if (!this._pagesCache[nPageIndex]) {
                this._releaseOldCachePages();
            }

            // add to cache
            this._pagesCache[nPageIndex] = {
                timestamp: Date.now(),
                data: arrHashes
            };

            await this._storage.setMainDagPageIndex(this._getDbRecordIndex(nPageIndex), arrHashes);
        }

        _releaseOldCachePages() {
            // delete old pages from cache if it's full
            if (Object.keys(this._pagesCache).length > Constants.MAIN_DAG_PAGES_IN_MEMORY - 1) {
                const arrOldIndexes = Object.entries(this._pagesCache)
                    .map(([key, value]) => ({
                        timestamp: value.timestamp,
                        index: key
                    }))
                    .sort((a, b) => a.timestamp - b.timestamp)
                    .slice(Constants.MAIN_DAG_PAGES_IN_MEMORY - 1)
                    .map(item => item.index);

                for (const index of arrOldIndexes) {
                    delete this._pagesCache[index];
                }
            }
        }

        _getDbRecordIndex(nPageIndex) {
            return `${this._strDagPrefix}_${nPageIndex}`;
        }
    };
};
