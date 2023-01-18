const factory = require('../../testFactory');

const {Uns} = require('./uns');
const {ADDRESS_TYPE, DID_PREFIX, PROVIDER} = require('./constants');

// const ADDRESS_IS_NOT_FOUND = 'Address is not found!';
// const ADDRESS_HAS_ALREADY_DEFINED = 'Address has already defined!';
const HASH_IS_NOT_FOUND = 'Hash is not found!';
const DID_DOCUMENT_HASH_ALREADY_DEFINED = 'DID document hash has already defined!';
const DID_DOCUMENT_DOESNT_HAVE_UNS_KEYS = 'DID document does not have UNS keys';
const UNS_HASH_ALREADY_DEFINED = 'Ubix NS hash has already defined';

class UnsDid extends Uns {
    constructor() {
        super();
        this._didDocuments = {};
        this._providers = PROVIDER.values();
    }

    getDidDocuments() {
        return this._didDocuments;
    }

    createDidDocument(didDocument) {
        if (!this._hasDocumentUnsKeys(didDocument)) {
            throw new Error(DID_DOCUMENT_DOESNT_HAVE_UNS_KEYS);
        }

        const address = factory.Crypto.createHash(didDocument);
        if (this.getDidDocuments[address]) {
            throw new Error(DID_DOCUMENT_HASH_ALREADY_DEFINED);
        }

        if (this._hasUnsKeys(didDocument)) {
            throw new Error(UNS_HASH_ALREADY_DEFINED);
        }

        this.getDidDocuments[address] = didDocument;

        this._createUnsKeys(didDocument, address);
    }

    replaceDidDocument(address, newDidDocument) {
        const oldDidDocument = this.getDidDocuments[address];
        if (!oldDidDocument) {
            throw new Error(HASH_IS_NOT_FOUND);
        }

        if (!this._hasDocumentUnsKeys(newDidDocument)) {
            throw new Error(DID_DOCUMENT_DOESNT_HAVE_UNS_KEYS);
        }

        // check except our own keys
        // if (this._hasUnsKeys(newDidDocument)) {
        //     throw new Error(UNS_HASH_ALREADY_DEFINED);
        // }

        // this._checkForUnsKeys(didDocument);

        const providers = PROVIDER.values();
        for (const key in oldDidDocument) {
            if (providers.includes(key)) {
                this._activeUns._remove(key, oldDidDocument[key]);
            }
        }

        this.getDidDocuments[address] = newDidDocument;

        this._createUnsKeys(newDidDocument, address);
    }

    removeDidDocument(address) {
        const didDocument = this.getDidDocuments[address];
        if (!didDocument) throw new Error(HASH_IS_NOT_FOUND);

        this._removeUnsKeys(didDocument);
    }

    getDidDocument(address) {
        return this._didDocument[address] || null;
    }

    getAddress(provider, name, addresType) {
        const hash = Uns.getNameForHash(provider, name);
        const address = this._hash2address[hash];

        if (!address) return null;

        switch (addresType) {
            case ADDRESS_TYPE.DID_ID:
                return UnsDid._getDid(address);
            case ADDRESS_TYPE.DID_DOCUMENT:
                return this.getDidDocument(address);
            default:
                return address;
        }
    }

    static _getDid(address) {
        return `${DID_PREFIX}:${address}`;
    }

    _hasDocumentUnsKeys(didDocument) {
        for (const key in didDocument) {
            if (this._providers.includes(key)) {
                return true;
            }
        }
        return false;
    }

    _hasUnsKeys(didDocument) {
        // тут если ключи и так нам принадлежат их не считать
        for (const key in didDocument) {
            if (this._providers.includes(key) && this.hasKey(key, didDocument[key])) {
                return true;
            }
        }
        return false;
    }

    _createUnsKeys(didDocument, address) {
        for (const key in didDocument) {
            if (this._providers.includes(key)) {
                this.add(key, didDocument[key], address);
            }
        }
    }

    _removeUnsKeys(didDocument) {
        for (const key in didDocument) {
            if (this._providers.includes(key)) {
                this._activeUns._remove(key, didDocument[key]);
            }
        }
    }
}

module.exports = {UnsDid};
