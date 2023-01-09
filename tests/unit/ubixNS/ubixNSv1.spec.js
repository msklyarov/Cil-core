'use strict';

const {describe, it} = require('mocha');
const {assert} = require('chai');

const {UbixNSv1: UnsContract} = require('./ubixNSv1');
const factory = require('../../testFactory');

const {generateAddress, pseudoRandomBuffer} = require('../../testUtil');

let contract;

describe('UbixNS', () => {
    before(async function () {
        this.timeout(15000);
        await factory.asyncLoad();
    });

    after(async function () {
        // this.timeout(15000);
    });

    beforeEach(async () => {
        global.value = 0;
        global.callerAddress = generateAddress().toString('hex');
        global.contractTx = pseudoRandomBuffer().toString('hex');
        // global.block = {
        //     height: 100,
        //     hash: 'hash'
        // };

        contract = new UnsContract();
    });

    describe('create Ubix NS record', async () => {
        let objUnsData;

        beforeEach(async () => {
            global.value = 130000;

            objUnsData = {
                strProvider: 'ubix',
                strName: 'mytestname',
                strIssuerName: 'Me',
                strDidAddress: '0x121212121212'
            };
        });

        it('should create', async () => {
            assert.equal(Object.keys(contract._data).length, 0);

            contract.create(objUnsData);

            assert.equal(Object.keys(contract._data).length, 1);
            assert.equal(
                contract.resolve(objUnsData.strProvider, objUnsData.strName).strDidAddress,
                objUnsData.strDidAddress
            );
        });

        it('should throw (unsigned TX)', async () => {
            global.callerAddress = undefined;
            assert.throws(() => contract.create(objUnsData), 'You should sign TX');
        });

        it('should throw (low create fee)', async () => {
            global.value = 130000 - 1;
            assert.throws(() => contract.create(objUnsData), 'Update fee is 130000');
        });

        it('should throw (strProvider should be a string)', async () => {
            assert.throws(() => contract.create({...objUnsData, strProvider: null}), 'strProvider should be a string');
        });

        it('should throw (strName should be a string)', async () => {
            assert.throws(() => contract.create({...objUnsData, strName: null}), 'strName should be a string');
        });

        it('should throw (strAddress should be a string)', async () => {
            assert.throws(
                () => contract.create({...objUnsData, strDidAddress: null}),
                'strDidAddress should be a string'
            );
            7;
        });

        it('should throw (create twice)', async () => {
            contract.create(objUnsData);
            assert.throws(() => contract.create(objUnsData), 'Hash has already defined');
        });
    });

    // TODO!!!! тут давать удалять только если это его!!!!

    describe('remove Ubix NS record', async () => {
        let objUnsData;

        beforeEach(async () => {
            global.value = 130000;

            objUnsData = {
                strProvider: 'ubix',
                strName: 'mytestname',
                strIssuerName: 'Me',
                strDidAddress: '0x121212121212'
            };
        });

        it('should remove', async () => {
            contract.create(objUnsData);

            assert.equal(Object.keys(contract._data).length, 1);
            contract.remove(objUnsData);
            assert.equal(Object.keys(contract._data).length, 0);
        });

        it('should throw (Hash is not found)', async () => {
            assert.throws(() => contract.remove(objUnsData), 'Hash is not found');
        });

        it('should throw (unsigned TX)', async () => {
            global.callerAddress = undefined;
            assert.throws(() => contract.remove(objUnsData), 'You should sign TX');
        });

        it('should throw (low create fee)', async () => {
            global.value = 130000 - 1;
            assert.throws(() => contract.remove(objUnsData), 'Update fee is 130000');
        });

        it('should throw (strProvider should be a string)', async () => {
            assert.throws(() => contract.remove({...objUnsData, strProvider: null}), 'strProvider should be a string');
        });

        it('should throw (strName should be a string)', async () => {
            assert.throws(() => contract.remove({...objUnsData, strName: null}), 'strName should be a string');
        });

        it('should throw (Unauthorized call)', async () => {
            contract.create(objUnsData);

            global.callerAddress = generateAddress().toString('hex');
            assert.throws(() => contract.remove(objUnsData), 'Unauthorized call');
        });
    });

    describe('resolve Ubix NS record', async () => {
        let objUnsData;

        beforeEach(async () => {
            global.value = 130000;

            objUnsData = {
                strProvider: 'ubix',
                strName: 'mytestname',
                strIssuerName: 'Me',
                strDidAddress: '0x121212121212'
            };

            contract.create(objUnsData);
        });

        it('should throw (Hash is not found)', async () => {
            assert.throws(() => contract.resolve('NO', 'NAME'), 'Hash is not found');
        });

        it('should throw (strProvider should be a string)', async () => {
            assert.throws(() => contract.resolve(null, objUnsData.strName), 'strProvider should be a string');
        });

        it('should throw (strName should be a string)', async () => {
            assert.throws(() => contract.resolve(objUnsData.strProvider, null), 'strName should be a string');
        });

        it('should pass', async () => {
            const {strIssuerName, strDidAddress} = contract.resolve(objUnsData.strProvider, objUnsData.strName);

            assert.isOk(strIssuerName);
            assert.isOk(strDidAddress);
        });
    });

    describe('create Ubix NS records in a batch mode', async () => {
        beforeEach(async () => {
            global.value = 130000;
        });

        it('should create', () => {
            const objDidDocument = {
                ubix: 'my_ubix_nick',
                email: 'my@best.mail',
                tg: 'john_doe'
            };
            const strIssuerName = 'Me';
            const strDidAddress = '0x121212121212';

            const keyMap = new Map(
                Object.entries(objDidDocument).map(([strProvider, strName]) => [
                    strProvider,
                    {
                        strName,
                        strIssuerName,
                        strDidAddress
                    }
                ])
            );

            contract.createBatch(keyMap);
            assert.equal(Object.keys(contract._data).length, Object.keys(objDidDocument).length);
        });

        it('should throw (Must be a Map instance)', () => {
            assert.throws(() => contract.createBatch(null), 'Must be a Map instance');
        });

        it('should throw (create twice)', async () => {
            const objDidDocument = {
                ubix: 'my_ubix_nick',
                email: 'my@best.mail',
                tg: 'john_doe'
            };
            const strIssuerName = 'Me';
            const strDidAddress = '0x121212121212';

            const keyMap = new Map(
                Object.entries(objDidDocument).map(([strProvider, strName]) => [
                    strProvider,
                    {
                        strName,
                        strIssuerName,
                        strDidAddress
                    }
                ])
            );

            contract.createBatch(keyMap);
            assert.throws(() => contract.createBatch(keyMap), 'Hash has already defined');
        });
    });

    describe('remove Ubix NS records in a batch mode', async () => {
        beforeEach(async () => {
            global.value = 130000;
        });

        it('should remove', () => {
            const objDidDocument = {
                ubix: 'my_ubix_nick',
                email: 'my@best.mail',
                tg: 'john_doe'
            };
            const strIssuerName = 'Me';
            const strDidAddress = '0x121212121212';

            const keyMap = new Map(
                Object.entries(objDidDocument).map(([strProvider, strName]) => [
                    strProvider,
                    {
                        strName,
                        strIssuerName,
                        strDidAddress
                    }
                ])
            );

            contract.createBatch(keyMap);
            assert.equal(Object.keys(contract._data).length, Object.keys(objDidDocument).length);
            contract.removeBatch(keyMap);
            assert.equal(Object.keys(contract._data).length, 0);
        });

        it('should throw (Must be a Map instance)', () => {
            const strAddress = 0x121212121212;
            assert.throws(() => contract.removeBatch({}, strAddress), 'Must be a Map instance');
        });

        it('should throw (Unauthorized call)', () => {
            const objDidDocument = {
                ubix: 'my_ubix_nick',
                email: 'my@best.mail',
                tg: 'john_doe'
            };
            const strIssuerName = 'Me';
            const strDidAddress = '0x121212121212';

            const keyMap = new Map(
                Object.entries(objDidDocument).map(([strProvider, strName]) => [
                    strProvider,
                    {
                        strName,
                        strIssuerName,
                        strDidAddress
                    }
                ])
            );

            contract.createBatch(keyMap);
            assert.equal(Object.keys(contract._data).length, Object.keys(objDidDocument).length);

            global.callerAddress = generateAddress().toString('hex');

            assert.throws(() => contract.removeBatch(keyMap), 'Unauthorized call');
        });
    });
});
