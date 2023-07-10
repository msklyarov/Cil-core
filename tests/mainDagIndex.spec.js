'use strict';

const {describe, it} = require('mocha');
const {assert} = require('chai');
const sinon = require('sinon').createSandbox();

const factory = require('./testFactory');
const {createDummyBlockInfo} = require('./testUtil');

let fakeResult = {
    fake: 1,
    toObject: function() {
        return this;
    },
    getHash: function() {
        return 'dead';
    }
};
let node;

describe('Main Dag', () => {
    before(async function() {
        this.timeout(15000);
        await factory.asyncLoad();
    });

    let storage;
    beforeEach(() => {
        node = {
            rpcHandler: sinon.fake.resolves(fakeResult)
        };
        storage = new factory.Storage();
    });

    after(async function() {
        this.timeout(15000);
    });

    it('should create instance', async () => {
        new factory.MainDagIndex({storage});
    });

    it('should rewrite vertex (add multiple times)', async () => {
        const dagIndex = new factory.MainDagIndex({storage});
        const bi = createDummyBlockInfo(factory);

        await dagIndex.addBlock(bi);

        // this block & parent
        assert.equal(await dagIndex.getOrder(), 2);
        // assert.equal(dagIndex.size, 1);

        await dagIndex.addBlock(bi);
        await dagIndex.addBlock(bi);
        await dagIndex.addBlock(bi);

        // this block & parent
        assert.equal(await dagIndex.getOrder(), 2);
        // assert.equal(dagIndex.size, 1);
    });
});
