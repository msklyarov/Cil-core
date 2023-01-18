const {Base} = require('../uns');

const NO_ACTIVE_UBIX_NS = 'There is no acitve Ubix NS!';

class DidProxy extends Base {
    constructor() {
        super();
        this._createFee = 1e10;
        this._unsList = []; // latest proxy contract is the actual
        this._activeUns = null; // in real contract, a contract address for invokeContract
    }

    add(unsDid) {
        this._activeUns = unsDid;
        this._unsList.push({date: new Date(), unsDid});
    }

    getDidDocuments() {
        if (!this._activeUns) throw new Error(NO_ACTIVE_UBIX_NS);
        return this._activeUns.getDidDocuments();
    }
}

module.exports = {
    DidProxy
};
