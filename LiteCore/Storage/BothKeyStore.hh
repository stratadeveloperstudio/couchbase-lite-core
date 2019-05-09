//
// BothKeyStore.hh
//
// Copyright © 2019 Couchbase. All rights reserved.
//

#pragma once
#include "KeyStore.hh"
#include "Query.hh"
#include "error.hh"

namespace litecore {

    /** A fake KeyStore that combines a real KeyStore for live documents and another for deleted ones. */
    class BothKeyStore : public KeyStore {
    public:
        BothKeyStore(KeyStore *liveStore, KeyStore *deadStore)
        :KeyStore(liveStore->dataFile(), liveStore->name(), liveStore->capabilities())
        ,_liveStore(liveStore)
        ,_deadStore(deadStore)
        {
            deadStore->shareSequencesWith(*liveStore);
        }


        //// Overrides:

        virtual void reopen() override              {_liveStore->reopen(); _deadStore->reopen();}
        virtual void close() override               {_liveStore->close(); _deadStore->close();}
        void shareSequencesWith(KeyStore&) override {Assert(false);}

        uint64_t recordCount() const override       {return _liveStore->recordCount();}
        sequence_t lastSequence() const override    {return _liveStore->lastSequence();}


        //// CRUD:

        virtual bool read(Record &rec, ContentOptions options = kDefaultContent) const override {
            return _liveStore->read(rec, options) || _deadStore->read(rec, options);
        }


        virtual Record get(sequence_t seq) const override {
            auto rec = _liveStore->get(seq);
            return rec.exists() ? rec :_deadStore->get(seq);
        }


        virtual sequence_t set(slice key, slice version, slice value,
                               DocumentFlags flags,
                               Transaction &t,
                               const sequence_t *replacingSequence =nullptr,
                               bool newSequence =true) override
        {
            bool deleting = (flags & DocumentFlags::kDeleted);
            auto target = (deleting ? _deadStore : _liveStore).get();   // the store to update
            auto other  = (deleting ? _liveStore : _deadStore).get();

            if (replacingSequence && *replacingSequence == 0) {
                // Request should succeed only if doc _doesn't_ exist yet, so check other KeyStore:
                bool exists = false;
                other->get(key, kMetaOnly, [&](const Record &rec) {
                    exists = rec.exists();
                });
                if (exists)
                    return 0;
            }

            // Forward the 'set' to the target store:
            auto seq = target->set(key, version, value, flags, t, replacingSequence, newSequence);
            
            if (seq > 0 && !replacingSequence) {
                // Have to manually nuke any older rev from the other store:
                // OPT: Try to avoid this!
                other->del(key, t);
            } else if (seq == 0 && replacingSequence && *replacingSequence > 0) {
                // Wrong sequence. Maybe record is currently in the other KeyStore; if so, delete it
                Assert(newSequence);
                if (other->del(key, t, *replacingSequence))
                    seq = target->set(key, version, value, flags, t, nullptr, newSequence);
            }
            return seq;
        }


        virtual bool del(slice key, Transaction &t, sequence_t replacingSequence) override {
            // Always delete from both stores, for safety's sake.
            bool a = _liveStore->del(key, t, replacingSequence);
            bool b = _deadStore->del(key, t, replacingSequence);
            return a || b;
        }


        virtual bool setDocumentFlag(slice key, sequence_t seq, DocumentFlags flags, Transaction &t) override {
            return _liveStore->setDocumentFlag(key, seq, flags, t)
                || _deadStore->setDocumentFlag(key, seq, flags, t);
        }


        virtual void transactionWillEnd(bool commit) override {
            _liveStore->transactionWillEnd(commit);
            _deadStore->transactionWillEnd(commit);
        }


        //// EXPIRATION:

        virtual bool setExpiration(slice key, expiration_t exp) override {
            return _liveStore->setExpiration(key, exp) || _deadStore->setExpiration(key, exp);
        }


        virtual expiration_t getExpiration(slice key) override {
            return std::max(_liveStore->getExpiration(key), _deadStore->getExpiration(key));
        }


        virtual expiration_t nextExpiration() override {
            auto lx = _liveStore->nextExpiration();
            auto dx = _deadStore->nextExpiration();
            if (lx > 0 && dx > 0)
                return std::min(lx, dx);        // choose the earliest time
            else
                return std::max(lx, dx);        // choose the nonzero time
        }


        virtual unsigned expireRecords(ExpirationCallback callback =nullptr) override {
            return _liveStore->expireRecords(callback) + _deadStore->expireRecords(callback);
        }


        //// QUERIES & INDEXES:

        // Enumeration is nontrivial; see impl in BothKeyStore.cc
        virtual RecordEnumerator::Impl* newEnumeratorImpl(bool bySequence,
                                                          sequence_t since,
                                                          RecordEnumerator::Options) override;

        virtual Retained<Query> compileQuery(slice expr, QueryLanguage language) override {
            return _liveStore->compileQuery(expr, language);
        }


        virtual bool supportsIndexes(IndexType type) const override {
            return _liveStore->supportsIndexes(type);
        }


        virtual bool createIndex(const IndexSpec &spec, const IndexOptions *options) override {
            return _liveStore->createIndex(spec, options);
        }


        virtual void deleteIndex(slice name) override {
            _liveStore->deleteIndex(name);
        }


        virtual std::vector<IndexSpec> getIndexes() const override {
            return _liveStore->getIndexes();
        }


    private:
        std::unique_ptr<KeyStore> _liveStore;
        std::unique_ptr<KeyStore> _deadStore;
    };


}
