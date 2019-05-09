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

    class BothEnumeratorImpl : public RecordEnumerator::Impl {
    public:
        BothEnumeratorImpl(RecordEnumerator::Impl *liveImpl,
                           RecordEnumerator::Impl *deadImpl,
                           bool bySequence)
        :_liveImpl(liveImpl)
        ,_deadImpl(deadImpl)
        ,_bySequence(bySequence)
        { }

        virtual bool next() override {
            if (!_first || _liveImpl.get() == _first) {
                if (!_liveImpl->next())
                    _liveImpl.reset();
            }
            if (!_first || _deadImpl.get() == _first) {
                if (!_deadImpl->next())
                    _deadImpl.reset();
            }

            bool liveIsFirst;
            if (_liveImpl && _deadImpl) {
                if (_bySequence)
                    liveIsFirst = _liveImpl->sequence() < _deadImpl->sequence();
                else
                    liveIsFirst = _liveImpl->key() < _deadImpl->key();
            } else if (_liveImpl || _deadImpl) {
                liveIsFirst = _liveImpl != nullptr;
            } else {
                _first = nullptr;
                return false;
            }

            _first = (liveIsFirst ? _liveImpl : _deadImpl).get();
            return true;
        }

        virtual bool read(Record &record) const override    {return _first->read(record);}
        virtual slice key() const override                  {return _first->key();}
        virtual sequence_t sequence() const override        {return _first->sequence();}

    private:
        unique_ptr<RecordEnumerator::Impl> _liveImpl, _deadImpl;
        RecordEnumerator::Impl* _first {nullptr};
        bool _bySequence;
    };


    RecordEnumerator::Impl* BothKeyStore::newEnumeratorImpl(bool bySequence,
                                                            sequence_t since,
                                                            RecordEnumerator::Options options)
    {
        if (options.includeDeleted) {
            auto liveImpl = _liveStore->newEnumeratorImpl(bySequence, since, options);
            auto deadImpl = _deadStore->newEnumeratorImpl(bySequence, since, options);
            return new BothEnumeratorImpl(liveImpl, deadImpl, bySequence);
        } else {
            return _liveStore->newEnumeratorImpl(bySequence, since, options);
        }
    }


}
