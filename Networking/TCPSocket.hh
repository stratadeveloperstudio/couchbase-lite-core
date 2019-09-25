//
// TCPSocket.hh
//
// Copyright © 2019 Couchbase. All rights reserved.
//

#pragma once
#include "RefCounted.hh"
#include "Address.hh"
#include "HTTPTypes.hh"
#include "function_ref.hh"
#include "fleece/Fleece.hh"
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace litecore { namespace crypto {
    class Cert;
}}
namespace litecore { namespace websocket {
    struct CloseStatus;
    class Headers;
} }
namespace sockpp {
    class socket;
    class stream_socket;
    class tls_context;
}

namespace litecore { namespace net {
    class HTTPLogic;

    /** TCP socket class, using the sockpp library. */
    class TCPSocket {
    public:
        using slice = fleece::slice;

        TCPSocket(bool isClient, sockpp::tls_context *ctx =nullptr);
        virtual ~TCPSocket();

        /// Initializes TCPSocket, must call at least once before using any
        /// socket related functionality
        static void initialize();

        /// Returns the TLS context, if any, used by this socket.
        sockpp::tls_context* TLSContext();

        /// Closes the socket if it's open.
        void close();

        bool connected() const;
        operator bool() const                   {return connected();}

        /// Peer's address: IP address + ":" + port number
        std::string peerAddress();

        /// Peer's TLS certificate (if it has one)
        fleece::Retained<crypto::Cert> peerTLSCertificate();

        /// Last error
        C4Error error() const                   {return _error;}

        //-------- READING:

        /// Reads up to \ref byteCount bytes to the location \ref dst.
        /// On EOF returns zero. On other error returns -1.
        ssize_t read(void *dst, size_t byteCount) MUST_USE_RESULT;

        /// Reads exactly \ref byteCount bytes to the location \ref dst.
        /// On premature EOF returns 0 and sets error {WebSocket, 400}.
        ssize_t readExactly(void *dst, size_t byteCount) MUST_USE_RESULT;

        static constexpr size_t kMaxDelimitedReadSize = 50 * 1024;

        /// Reads from the socket until the \ref delimiter byte sequence is found,
        /// and returns the bytes read ending with the delimiter.
        /// If the delimiter is not found, due to EOF of reading more than \ref maxSize bytes,
        /// throws an exception.
        fleece::alloc_slice readToDelimiter(slice delimiter,
                                            bool includeDelimiter =true,
                                            size_t maxSize =kMaxDelimitedReadSize) MUST_USE_RESULT;

        /// Reads an HTTP body, given the headers.
        /// If there's a Content-Length header, reads that many bytes, otherwise reads till EOF.
        bool readHTTPBody(const websocket::Headers &headers, fleece::alloc_slice &body) MUST_USE_RESULT;

        bool atReadEOF() const                          {return _eofOnRead;}

        //-------- WRITING:

        /// Writes to the socket and returns the number of bytes written:
        ssize_t write(slice) MUST_USE_RESULT;

        /// Writes all the bytes to the socket.
        ssize_t write_n(slice) MUST_USE_RESULT;

        /// Writes multiple byte ranges (slices) to the socket.
        /// Those that are completely written are removed from the head of the vector.
        /// One that's partially written has its `buf` and `size` adjusted to cover only the
        /// unsent bytes. (This will always be the 1st in the vector on return.)
        ssize_t write(std::vector<fleece::slice> &ioByteRanges) MUST_USE_RESULT;

        bool atWriteEOF() const                         {return _eofOnWrite;}

        //-------- [NON]BLOCKING AND WAITING:

        /// Sets read/write/connect timeout in seconds
        bool setTimeout(double secs);
        double timeout() const                  {return _timeout;}

        /// Enables or disables non-blocking mode.
        bool setNonBlocking(bool);

        using interruption_t = uint8_t;

        /// Blocks until the socket has data to read (if `ioReadable` is true) and/or has
        /// space for output (if `ioWriteable` is true.) On return, `ioReadable` and `ioWriteable`
        /// will be set according to which condition is now true.
        /// If `interruptWait()` was called, `outMessage` will be set to the interruption
        /// message it was called with. Otherwise `outMessage` is zero on return.
        bool waitForIO(bool &ioReadable, bool &ioWriteable, interruption_t &outMessage);

        /// Interrupts a `waitForIO()` call on another thread. The given interruption message
        /// will be set as the `outMessage` parameter when `waitForIO` returns.
        /// If `waitForIO()` is not currently running, then the next call will immediately
        /// be interrupted, with this message.
        bool interruptWait(interruption_t);

    protected:
        bool setSocket(std::unique_ptr<sockpp::stream_socket>);
        void setError(C4ErrorDomain, int code, slice message =fleece::nullslice);
        bool wrapTLS(slice hostname);
        bool createInterruptPipe();
        void checkStreamError();
        bool checkSocketFailure();
        bool checkSocket(const sockpp::socket &sock);
        ssize_t _read(void *dst, size_t byteCount) MUST_USE_RESULT;
        void pushUnread(slice);

    private:
        bool _setTimeout(double secs);
        
        std::unique_ptr<sockpp::stream_socket> _socket;     // The TCP (or TLS) socket
        sockpp::stream_socket* _wrappedSocket {nullptr};    // Underlying TLS socket, when using TCP
        sockpp::tls_context* _tlsContext {nullptr};         // Custom TLS context if any
        bool _isClient;
        bool _nonBlocking {false};                          // Is socket in non-blocking mode?
        double _timeout {0};                                // read/write/connect timeout in seconds
        C4Error _error {};                                  // last error
        fleece::alloc_slice _unread;        // Data read from socket that's been "pushed back"
        size_t _unreadLen {0};              // Length of valid data in _unread
        bool _eofOnRead {false};            // Has read stream reached EOF?
        bool _eofOnWrite {false};           // Has write stream reached EOF?
        int _interruptReadFD {-1};          // File descriptor of pipe used to interrupt select()
        int _interruptWriteFD {-1};         // Other end of the pipe used to interrupt select()
        std::mutex _mutex;                  // Synchronizes creation of the above FDs
    };



    /** A client socket, that opens a TCP connection. */
    class ClientSocket : public TCPSocket {
    public:
        ClientSocket(sockpp::tls_context* =nullptr);

        /// Connects to the host, synchronously. On failure throws an exception.
        bool connect(const Address &addr) MUST_USE_RESULT;

        /// Wrap the existing socket in TLS, performing a handshake.
        /// This is used after connecting to a CONNECT-type proxy, not in a normal connection.
        bool wrapTLS(slice hostname)        {return TCPSocket::wrapTLS(hostname);}
    };

    

    /** A server-side socket, that handles a client connection. */
    class ResponderSocket : public TCPSocket {
    public:
        ResponderSocket(sockpp::tls_context* =nullptr);

        bool acceptSocket(sockpp::stream_socket&&) MUST_USE_RESULT;
        bool acceptSocket(std::unique_ptr<sockpp::stream_socket>) MUST_USE_RESULT;

        /// Perform server-side TLS handshake.
        bool wrapTLS()                      {return TCPSocket::wrapTLS(fleece::nullslice);}
    };


} }
