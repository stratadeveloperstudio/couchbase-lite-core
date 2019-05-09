//
// BothKeyStore.cc
//
// Copyright © 2019 Couchbase. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "BothKeyStore.hh"
#include "RecordEnumerator.hh"
#include <memory>

namespace litecore {
    using namespace std;

    // Enumerator implementation for BothKeyStore. It enumerates both KeyStores in parallel,
    // always returning the lowest-sorting record (basically a merge-sort.)
    class BothEnumeratorImpl : public RecordEnumerator::Impl {
    public:
        BothEnumeratorImpl(bool bySequence,
                           sequence_t since,
                           RecordEnumerator::Options options,
                           KeyStore *liveStore, KeyStore *deadStore)
        :_liveImpl(liveStore->newEnumeratorImpl(bySequence, since, options))
        ,_deadImpl(deadStore->newEnumeratorImpl(bySequence, since, options))
        ,_bySequence(bySequence)
        { }

        virtual bool next() override {
            // Advance the enumerator whose value was used last:
            if (_current == nullptr || _current == _liveImpl.get()) {
                if (!_liveImpl->next())
                    _liveImpl.reset();
            }
            if (_current == nullptr || _current == _deadImpl.get()) {
                if (!_deadImpl->next())
                    _deadImpl.reset();
            }

            // Pick the enumerator with the lowest key/sequence to be used next:
            bool useLive;
            if (_liveImpl && _deadImpl) {
                if (_bySequence)
                    useLive = _liveImpl->sequence() < _deadImpl->sequence();
                else
                    useLive = _liveImpl->key() < _deadImpl->key();
            } else if (_liveImpl || _deadImpl) {
                useLive = _liveImpl != nullptr;
            } else {
                _current = nullptr;
                return false;
            }

            _current = (useLive ? _liveImpl : _deadImpl).get();
            return true;
        }

        virtual bool read(Record &record) const override    {return _current->read(record);}
        virtual slice key() const override                  {return _current->key();}
        virtual sequence_t sequence() const override        {return _current->sequence();}

    private:
        unique_ptr<RecordEnumerator::Impl> _liveImpl, _deadImpl;    // Real enumerators
        RecordEnumerator::Impl* _current {nullptr};                 // Enumerator w/lowest key
        bool _bySequence;                                           // Sorting by sequence?
    };


    RecordEnumerator::Impl* BothKeyStore::newEnumeratorImpl(bool bySequence,
                                                            sequence_t since,
                                                            RecordEnumerator::Options options)
    {
        if (options.includeDeleted) {
            return new BothEnumeratorImpl(bySequence, since, options,
                                          _liveStore.get(), _deadStore.get());
        } else {
            return _liveStore->newEnumeratorImpl(bySequence, since, options);
        }
    }


}
