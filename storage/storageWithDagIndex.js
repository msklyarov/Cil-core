'use strict';

const levelup = require('levelup');
const leveldown = require('leveldown');
const util = require('util');
const debugLib = require('debug');

const debug = debugLib('storage:dag:');
const levelDbDestroy = util.promisify(leveldown.destroy);

module.exports = (PersistentStorage, factory) => {
    const {Constants} = factory;

    return class StorageWithDagIndex extends PersistentStorage {
        constructor(options) {
            super(options);
            this._initMainDagIndexDb();
        }

        _initMainDagIndexDb() {
            this._mainDagIndexStorage = levelup(
                this._downAdapter(`${this._pathPrefix}/${Constants.DB_MAIN_DAG_INDEX_DIR}`)
            );
        }

        async dropAllForReIndex(bEraseBlockStorage = false) {
            await super.dropAllForReIndex(bEraseBlockStorage);

            if (typeof this._downAdapter.destroy === 'function') {
                await this.close();
                await levelDbDestroy(`${this._pathPrefix}/${Constants.DB_MAINDAG_INDEX_DIR}`);
            }
        }

        async close() {
            if (this._mainDagIndexStorage) await this._mainDagIndexStorage.close();
        }

        async getMainDagPageIndex(nPageIndex) {
            const lock = await this._mutex.acquire(['dagIndexPage']);

            try {
                const strResult = await this._mainDagIndexStorage.get(nPageIndex.toString()).catch(err => debug(err));
                if (!strResult) return null;
                return JSON.parse(strResult.toString());
            } finally {
                this._mutex.release(lock);
            }
        }

        async setMainDagPageIndex(nPageIndex, arrHashes) {
            const lock = await this._mutex.acquire(['dagIndexPage']);

            try {
                await this._mainDagIndexStorage.put(nPageIndex.toString(), JSON.stringify(arrHashes));
            } finally {
                this._mutex.release(lock);
            }
        }

        async getMainDagIndexOrder() {
            const lock = await this._mutex.acquire(['dagIndexOrder']);

            try {
                const result = await this._mainDagIndexStorage.get('order').catch(err => debug(err));
                return result ? +result.toString() : 0;
            } finally {
                this._mutex.release(lock);
            }
        }

        async incMainDagIndexOrder() {
            const lock = await this._mutex.acquire(['dagIndexOrder']);

            try {
                const result = await this._mainDagIndexStorage.get('order').catch(err => debug(err));
                await this._mainDagIndexStorage.put('order', (result ? +result.toString() : 0) + 1);
            } finally {
                this._mutex.release(lock);
            }
        }

        async getBlockInfoNoThrow(strHash) {
            try {
                return await this._storage.getBlockInfo(strHash);
            } catch {
                return null;
            }
        }
    };
};
