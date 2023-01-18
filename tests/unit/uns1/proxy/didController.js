const factory = require('../../../testFactory');
const {DidProxy} = require('./didProxy');
const {PROVIDER} = require('../constants');

const HASH_IS_NOT_FOUND = 'Hash is not found!';
const DID_DOCUMENT_HASH_ALREADY_DEFINED = 'DID document hash has already defined!';
const DID_DOCUMENT_DOESNT_HAVE_UNS_KEYS = 'DID document does not have UNS keys';
const UNS_HASH_ALREADY_DEFINED = 'Ubix NS hash has already defined';

class DidController extends DidProxy {
    constructor() {
        super();
        this._providers = PROVIDER.values();
    }

    create(didDocument) {
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

    replace(address, newDidDocument) {
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

    remove(address) {
        const didDocument = this.getDidDocuments[address];
        if (!didDocument) throw new Error(HASH_IS_NOT_FOUND);

        this._removeUnsKeys(didDocument);
    }

    _hasDocumentUnsKeys(didDocument) {
        // check DID document for UNS keys
        let hasUnsKey = false;
        const providers = PROVIDER.values();
        for (const key in didDocument) {
            if (providers.includes(key)) {
                hasUnsKey = true;
                break;
            }
        }

        if (!hasUnsKey) throw new Error(DID_DOCUMENT_DOESNT_HAVE_UNS_KEYS);
    }

    _checkForUnsKeys(didDocument) {}

    _createUnsKeys(didDocument, address) {
        for (const key in didDocument) {
            if (this._providers.includes(key)) {
                // а что если такой ключ уже есть?
                this._activeUns._add(key, didDocument[key], address);
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

module.exports = {
    DidController
};
