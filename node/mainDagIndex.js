'use strict';

const assert = require('assert');
const typeforce = require('typeforce');

const types = require('../types');

module.exports = () =>
    class MainDagIndex {
        constructor(props) {
            const { storage } = props;
            assert(storage, 'MainDagIndex constructor requires Storage instance!');

            this._storage = storage;
        }

        async addBlock(blockInfo) {
            typeforce(types.BlockInfo, blockInfo);

            await this._storage.addMainDagIndex(blockInfo);
        }

        async isProcessed(strHash) {
            typeforce(types.Str64, strHash);

            return !!(((await this._storage.getMainDagIndex(strHash)) || {}).processed);
        }

        async getOrder() {
            return await this._storage.getMainDagIndexOrder();
        }

        /**
         * @param {String} hash
         * @returns {Array}  of blocks that are children of a block with hash
         */
        async getChildren(strHash) {
            typeforce(types.Str64, strHash);

            return ((await this._storage.getMainDagIndex(strHash)) || {}).children || [];
        }
    };
