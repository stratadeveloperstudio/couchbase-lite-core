//
// BuiltInWebSocket.cc
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

#include "BuiltInWebSocket.hh"
#include "HTTPLogic.hh"
#include "Certificate.hh"
#include "CookieStore.hh"
#include "c4Replicator.h"
#include "c4Socket+Internal.hh"
#include "c4.hh"
#include "Error.hh"
#include "StringUtil.hh"
#include "ThreadUtil.hh"
#include "sockpp/mbedtls_context.h"
#include "sockpp/exception.h"
#include <string>

using namespace litecore;
using namespace litecore::websocket;


void C4RegisterBuiltInWebSocket() {
    repl::C4SocketImpl::registerInternalFactory([](websocket::URL url,
                                                   fleece::alloc_slice options,
                                                   C4Database *database) -> WebSocketImpl*
                                                {
        return new BuiltInWebSocket(url, fleece::AllocedDict(options), database);
    });
}


namespace litecore { namespace websocket {
    using namespace std;
    using namespace fleece;
    using namespace net;


    enum Interruption : TCPSocket::interruption_t {
        kReadableInterrupt      = 1,    // read capacity is now > 0
        kWriteableInterrupt     = 2,    // there are now messages to send
        kCloseInterrupt         = 255   // time to close the socket
    };


    // private shared constructor
    BuiltInWebSocket::BuiltInWebSocket(const URL &url,
                                       Role role,
                                       const fleece::AllocedDict &options)
    :WebSocketImpl(url, role, options, true)
    ,_readBuffer(kReadBufferSize)
    {
        TCPSocket::initialize();
    }


    // client constructor
    BuiltInWebSocket::BuiltInWebSocket(const URL &url,
                                       const fleece::AllocedDict &options,
                                       C4Database *database)
    :BuiltInWebSocket(url, Role::Client, options)
    {
        _database = c4db_retain(database);
    }


    // server constructor
    BuiltInWebSocket::BuiltInWebSocket(const URL &url,
                                       unique_ptr<net::ResponderSocket> socket)
    :BuiltInWebSocket(url, Role::Server, AllocedDict())
    {
        _socket.reset(socket.release());
    }


    BuiltInWebSocket::~BuiltInWebSocket() {
        logDebug("~BuiltInWebSocket");
    }


    void BuiltInWebSocket::connect() {
        // Spawn a thread to connect and run the read loop:
        WebSocketImpl::connect();
        retain(this);
        _ioThread = thread(bind(&BuiltInWebSocket::run, this));
        _ioThread.detach();
    }


    void BuiltInWebSocket::closeSocket() {
        logVerbose("closeSocket");
        if (_socket)
            _socket->interruptWait(kCloseInterrupt);
    }


    void BuiltInWebSocket::sendBytes(alloc_slice bytes) {
        unique_lock<mutex> lock(_outboxMutex);
        if (_outbox.empty() && _waitingForIO)
            _socket->interruptWait(kWriteableInterrupt);
        _outboxAlloced.push_back(bytes);
        _outbox.push_back(bytes);
    }


    void BuiltInWebSocket::receiveComplete(size_t byteCount) {
        size_t oldCapacity = _curReadCapacity.fetch_add(byteCount);
        Assert(oldCapacity + byteCount <= kReadCapacity);
        if (oldCapacity == 0)
            logDebug("**** socket read RESUMED");
        if (oldCapacity == 0 && _waitingForIO)
            _socket->interruptWait(kReadableInterrupt);
    }


    void BuiltInWebSocket::requestClose(int status, fleece::slice message) {
        Assert(false, "Should not be called");
    }


#pragma mark - CONNECTING:


    // This runs on its own thread.
    void BuiltInWebSocket::run() {
        setThreadName();

        if (!_socket) {
            try {
                // Connect:
                auto socket = _connectLoop();
                if (!socket) {
                    release(this);
                    return;
                }

                _socket = move(socket);
            } catch (const std::exception &x) {
                closeWithException(x, "while connecting");
                release(this);
                return;
            }
        }

        // OK, now we are connected -- notify delegate and start the loop for I/O:
        onConnect();
        ioLoop();
    }


    void BuiltInWebSocket::setThreadName() {
        stringstream name;
        name << "CBL WebSocket " << (role() == Role::Client ? "to " : "from ");
        Address addr(url());
        name << addr.hostname << ":" << addr.port;
        SetThreadName(name.str().c_str());
    }


    unique_ptr<ClientSocket> BuiltInWebSocket::_connectLoop() {
        Dict authDict = options()[kC4ReplicatorOptionAuthentication].asDict();
        slice authType = authDict[kC4ReplicatorAuthType].asString();

        // Custom TLS context:
        slice rootCerts = options()[kC4ReplicatorOptionRootCerts].asData();
        slice pinnedCert = options()[kC4ReplicatorOptionPinnedServerCert].asData();
        if (rootCerts || pinnedCert || authType == slice(kC4AuthTypeClientCert)) {
            _tlsContext.reset(new sockpp::mbedtls_context);
            if (rootCerts)
                _tlsContext->set_root_certs(string(rootCerts));
            if (pinnedCert)
                _tlsContext->allow_only_certificate(string(pinnedCert));
            if (authType == slice(kC4AuthTypeClientCert)) {
                if (!configureClientCert(authDict))
                    return nullptr;
            }
        }

        // Create the HTTPLogic object:
        Dict headers = options()[kC4ReplicatorOptionExtraHeaders].asDict();
        HTTPLogic logic {Address(url()), Headers(headers)};
        logic.setCookieProvider(this);
        logic.setWebSocketProtocol(options()[kC4SocketOptionWSProtocols].asString());

        if (!configureProxy(logic, options()[kC4ReplicatorOptionProxyServer].asDict())) {
            closeWithError(c4error_make(LiteCoreDomain, kC4ErrorInvalidParameter,
                                        "Invalid/unsupported proxy settings"_sl));
            return nullptr;
        }

        // Now send the HTTP request(s):
        bool usedAuth = false;
        unique_ptr<ClientSocket> socket;
        HTTPLogic::Disposition lastDisposition = HTTPLogic::kFailure;
        while (true) {
            if (lastDisposition != HTTPLogic::kContinue) {
                socket = make_unique<ClientSocket>(_tlsContext.get());
                socket->setTimeout(kConnectTimeoutSecs);
            }
            switch (logic.sendNextRequest(*socket)) {
                case HTTPLogic::kSuccess:
                    gotHTTPResponse(int(logic.status()), logic.responseHeaders());
                    socket->setTimeout(0);
                    return socket;
                case HTTPLogic::kRetry:
                    break; // redirected; go around again
                case HTTPLogic::kContinue:
                    break; // Will continue with the same socket (after connecting to a proxy)
                case HTTPLogic::kAuthenticate: {
                    if (!usedAuth && authType == slice(kC4AuthTypeBasic)
                                  && !logic.authChallenge()->forProxy
                                  && logic.authChallenge()->type == "Basic") {
                        slice username = authDict[kC4ReplicatorAuthUserName].asString();
                        slice password = authDict[kC4ReplicatorAuthPassword].asString();
                        if (username && password) {
                            logic.setAuthHeader(HTTPLogic::basicAuth(username, password));
                            usedAuth = true;
                            break; // retry with credentials
                        }
                    }
                    // give up:
                    gotHTTPResponse(int(logic.status()), logic.responseHeaders());
                    closeWithError(c4error_make(WebSocketDomain, int(logic.status()), nullslice));
                    return nullptr;
                }
                case HTTPLogic::kFailure:
                    if (logic.status() != HTTPStatus::undefined)
                        gotHTTPResponse(int(logic.status()), logic.responseHeaders());
                    closeWithError(logic.error());
                    return nullptr;
            }
        }
    }


    bool BuiltInWebSocket::configureClientCert(Dict auth) {
        try {
            slice certData = auth[kC4ReplicatorAuthClientCert].asData();
            if (!certData) {
                closeWithError(c4error_make(LiteCoreDomain, kC4ErrorInvalidParameter,
                                            "Missing TLS client cert in C4Replicator config"_sl));
                return false;
            }
            if (slice keyData = auth[kC4ReplicatorAuthClientCertKey].asData(); keyData) {
                _tlsContext->set_identity(string(certData), string(keyData));
                return true;
            } else {
#ifdef PERSISTENT_PRIVATE_KEY_AVAILABLE
                Retained<crypto::Cert> cert = new crypto::Cert(certData);
                Retained<crypto::PrivateKey> key = cert->loadPrivateKey();
                if (!key) {
                    closeWithError(c4error_make(LiteCoreDomain, kC4ErrorCrypto,
                                                "Couldn't find private key for identity cert"_sl));
                    return false;
                }
                auto mbedContext = (sockpp::mbedtls_context*)_tlsContext.get();
                mbedContext->set_identity(cert->context(), key->context());
                _tlsIdentity = new crypto::Identity(cert, key);
                return true;
#else
                closeWithError(c4error_make(LiteCoreDomain, kC4ErrorInvalidParameter,
                                            "Missing TLS private key in C4Replicator config"_sl));
                return false;
#endif
            }
        } catch (const std::exception &x) {
            closeWithException(x, "configuring TLS client certificate");
            return false;
        }
    }


    bool BuiltInWebSocket::configureProxy(HTTPLogic &logic, Dict proxyOpt) {
        if (!proxyOpt)
            return true;
        slice typeStr = proxyOpt[kC4ReplicatorProxyType].asString();
        if (typeStr == nullslice || typeStr == slice(kC4ProxyTypeNone)) {
            logic.setProxy({});
        } else {
            ProxyType type;
            if (typeStr == slice(kC4ProxyTypeHTTP))
                type = ProxyType::HTTP;
            else if (typeStr == slice(kC4ProxyTypeHTTPS))
                type = ProxyType::HTTPS;
            else
                return false;
            ProxySpec proxy(type,
                            proxyOpt[kC4ReplicatorProxyHost].asString(),
                            uint16_t(proxyOpt[kC4ReplicatorProxyPort].asInt()));
            Dict auth = proxyOpt[kC4ReplicatorProxyAuth].asDict();
            if (auth) {
                proxy.username = auth[kC4ReplicatorAuthUserName].asString();
                proxy.password = auth[kC4ReplicatorAuthPassword].asString();
                if (!proxy.username)
                    return false;
            }
            logic.setProxy(proxy);
        }
        return true;
    }


    alloc_slice BuiltInWebSocket::cookiesForRequest(const Address &addr) {
        alloc_slice cookies(c4db_getCookies(_database, addr, nullptr));

        slice cookiesOption = options()[kC4ReplicatorOptionCookies].asString();
        if (cookiesOption) {
            Address dstAddr(url());
            Cookie ck(string(cookiesOption),
                      string(slice(dstAddr.hostname)), string(slice(dstAddr.path)));
            if (ck.valid() && ck.matches(addr) && !ck.expired()) {
                if (cookies)
                    cookies.append("; "_sl);
                cookies.append(cookiesOption);
            }
        }
        return cookies;
    }


    void BuiltInWebSocket::setCookie(const Address &addr, slice cookieHeader) {
        c4db_setCookie(_database, cookieHeader, addr.hostname, addr.path, nullptr);
    }


#pragma mark - I/O:


    void BuiltInWebSocket::ioLoop() {
        try {
            if (_socket->setNonBlocking(true)) {
                while (true) {
                    _waitingForIO = true;
                    bool readable = (_curReadCapacity > 0);
                    bool writeable;
                    {
                        unique_lock<mutex> lock(_outboxMutex);
                        writeable = !_outbox.empty();
                    }

                    TCPSocket::interruption_t interruption;
                    if (!_socket->waitForIO(readable, writeable, interruption))
                        break;

                    _waitingForIO = false;
                    if (interruption == kCloseInterrupt)
                        break;
                    if (readable || interruption == kReadableInterrupt) {
                        if (!readFromSocket())
                            break;
                    }
                    if (writeable || interruption == kWriteableInterrupt) {
                        if (!writeToSocket())
                            break;
                    }
                }
            }
            closeWithError(_socket->error());

        } catch (const exception &x) {
            closeWithException(x, "during I/O");
        }

        _socket->close();
        release(this);
    }


    bool BuiltInWebSocket::readFromSocket() {
        ssize_t n = _socket->read((void*)_readBuffer.buf, min(_readBuffer.size,
                                                              _curReadCapacity.load()));
        logDebug("Received %zu bytes from socket", n);
        if (_usuallyFalse(n <= 0))
            return (n == 0);

        // The bytes read count against the read-capacity:
        auto oldCapacity = _curReadCapacity.fetch_sub(n);
        if (oldCapacity - n == 0)
            logDebug("**** socket read THROTTLED");

        // Pass data to WebSocket parser:
        onReceive(slice(_readBuffer.buf, n));
        return true;
    }


    bool BuiltInWebSocket::writeToSocket() {
        // Copy the outbox -- it's just a vector of {ptr,size} pairs, no biggie -- so we don't have
        // to hold the mutex while writing. (Even though the write won't actually block.)
        vector<slice> outboxSnapshot;
        {
            unique_lock<mutex> lock(_outboxMutex);
            outboxSnapshot = _outbox;
        }
        size_t beforeSize = outboxSnapshot.size();

        // Now write the data:
        ssize_t n = _socket->write(outboxSnapshot);
        if (n <= 0)
            return n == 0;

        size_t nRemoved = beforeSize - outboxSnapshot.size();
        {
            // After writing, sync _outbox & _outboxAlloced with the changes made to outboxSnapshot.
            // First remove the items written:
            unique_lock<mutex> lock(_outboxMutex);
            _outboxAlloced.erase(_outboxAlloced.begin(), _outboxAlloced.begin() + nRemoved);
            _outbox.erase(_outbox.begin(), _outbox.begin() + nRemoved);
            // Then copy the the first remaining item, in case its start ptr was advanced:
            if (!outboxSnapshot.empty())
                _outbox[0] = outboxSnapshot[0];
        }

        // Notify that data's been written:
        logDebug("Wrote %zu bytes to socket, in %zu (of %zu) messages",
                 n, nRemoved, outboxSnapshot.size()+nRemoved);
        onWriteComplete(n);
        return true;
    }


#pragma mark - ERRORS:


    void BuiltInWebSocket::closeWithException(const exception &x, const char *where) {
        // Convert exception to CloseStatus:
        logError("caught exception %s: %s", where, x.what());
        error e = error::convertException(x);
        closeWithError(c4error_make(C4ErrorDomain(e.domain), e.code, slice(e.what())));
    }


    void BuiltInWebSocket::closeWithError(C4Error err) {
        if (err.code == 0) {
            onClose(0);
        } else {
            alloc_slice message(c4error_getMessage(err));
            CloseStatus status {kUnknownError, err.code, message};
            if (err.domain == WebSocketDomain)
                status.reason = kWebSocketClose;
            else if (err.domain == POSIXDomain)
                status.reason = kPOSIXError;
            else if (err.domain == NetworkDomain)
                status.reason = kNetworkError;
            onClose(status);
        }
    }

} }
