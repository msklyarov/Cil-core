const {DidProxy} = require('./didProxy');
const {ADDRESS_TYPE} = require('../constants');

class DidDocument extends DidProxy {
    constructor() {
        super();
    }

    getByAddress(address) {
        return this.getDidDocuments()[address];
    }

    get(provider, name) {
        // invoke a contract here
        return this._activeUns._get(provider, name, ADDRESS_TYPE.DID_DOCUMENT);
    }
}

module.exports = {
    DidDocument
};
