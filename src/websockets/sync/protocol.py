from __future__ import annotations

import logging
import random
import socket
import struct
import threading
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Union, cast

from ..connection import OPEN, Connection, Event
from ..exceptions import ConnectionClosedOK
from ..frames import BytesLike, Frame, prepare_ctrl
from ..http11 import Request, Response
from ..typing import Data
from .messages import Assembler


__all__ = ["Protocol"]

logger = logging.getLogger(__name__)


class Protocol:
    def __init__(
        self,
        sock: socket.socket,
        connection: Connection,
        ping_interval: Optional[float] = None,
        ping_timeout: Optional[float] = None,
        close_timeout: Optional[float] = None,
    ) -> None:
        self.sock = sock
        self.connection = connection
        if ping_interval is not None or ping_timeout is not None:
            raise NotImplementedError("keepalive isn't implemented")
        self.close_timeout = close_timeout

        # Inject reference to this instance in the connection's logger.
        # https://github.com/python/typeshed/issues/5561
        self.connection.logger = cast(logging.Logger, self.connection.logger)
        self.connection.logger = logging.LoggerAdapter(
            self.connection.logger, {"websocket": self}
        )

        # Copy attributes from the connection for convenience.
        self.logger = self.connection.logger
        self.debug = self.connection.debug

        # HTTP handshake request and response.
        self.request: Optional[Request] = None
        self.response: Optional[Response] = None

        # Mutex serializing interactions with the connection.
        self.conn_mutex = threading.Lock()

        # Transforming frames into messages.
        self.messages = Assembler()

        # Mapping of ping IDs to pong waiters, in chronological order.
        self.pings: Dict[bytes, threading.Event] = {}

        # Receiving events from the socket.
        self.recv_events_thread = threading.Thread(target=self.recv_events)
        self.recv_events_thread.start()
        self.recv_events_exc: Optional[BaseException] = None

        # Closing the TCP connection.
        self.tcp_close_lock = threading.Lock()
        self.tcp_close_thread = threading.Thread(target=self.tcp_close)

    # Public attributes

    @property
    def id(self) -> Any:
        """
        Unique identifier. For logs.

        """
        return self.connection.id

    @property
    def local_address(self) -> Any:
        """
        Local address of the connection as a ``(host, port)`` tuple.

        """
        return self.sock.getsockname()

    @property
    def remote_address(self) -> Any:
        """
        Remote address of the connection as a ``(host, port)`` tuple.

        """
        return self.sock.getpeername()

    # Public methods

    def __iter__(self) -> Iterator[Data]:
        """
        Iterate on received messages.

        Exit normally when the connection is closed with code 1000 or 1001.

        Raise an exception in other cases.

        """
        try:
            while True:
                data = self.recv()
                assert data is not None, "recv without timeout cannot return None"
                yield data
        except ConnectionClosedOK:
            return

    def recv(self, timeout: Optional[float] = None) -> Optional[Data]:
        """
        Receive the next message.

        Return a :class:`str` for a text frame and :class:`bytes` for a binary
        frame.

        When the end of the message stream is reached, :meth:`recv` raises
        :exc:`~websockets.exceptions.ConnectionClosed`. Specifically, it
        raises :exc:`~websockets.exceptions.ConnectionClosedOK` after a normal
        connection closure and
        :exc:`~websockets.exceptions.ConnectionClosedError` after a protocol
        error or a network failure.

        If ``timeout`` is ``None``, block until a message is received. Else,
        if no message is received within ``timeout`` seconds, return ``None``.
        Set ``timeout`` to ``0`` to check if a message was already received.

        :raises ~websockets.exceptions.ConnectionClosed: when the
            connection is closed
        :raises RuntimeError: if two threads call :meth:`recv` or
            :meth:`recv_streaming` concurrently

        """
        try:
            return self.messages.get(timeout)
        except EOFError:
            raise self.connection.close_exc
        except RuntimeError:
            raise RuntimeError(
                "cannot call recv() while another thread "
                "is already running recv() or recv_streaming()"
            )

    def recv_streaming(self) -> Iterator[Data]:
        """
        Receive the next message frame by frame.

        Return an iterator of :class:`str` for a text frame and :class:`bytes`
        for a binary frame. The iterator must be consumed entirely, or else
        the connection will become unusable.

        With the exception of the return value, :meth:`recv_streaming` behaves
        like :meth:`recv`.

        """
        try:
            yield from self.messages.get_iter()
        except EOFError:
            raise self.connection.close_exc
        except RuntimeError:
            raise RuntimeError(
                "cannot call recv_streaming() while another thread "
                "is already running recv() or recv_streaming()"
            )

    def send(self, message: Union[Data, Iterable[Data]]) -> None:
        """
        Send a message.

        A string (:class:`str`) is sent as a text frame. A bytestring or
        bytes-like object (:class:`bytes`, :class:`bytearray`, or
        :class:`memoryview`) is sent as a binary frame.

        :meth:`send` also accepts an iterable of strings, bytestrings, or
        bytes-like objects. In that case the message is fragmented. Each item
        is treated as a message fragment and sent in its own frame. All items
        must be of the same type, or else :meth:`send` will raise a
        :exc:`TypeError` and the connection will be closed.

        :meth:`send` rejects dict-like objects because this is often an error.
        If you wish to send the keys of a dict-like object as fragments, call
        its :meth:`~dict.keys` method and pass the result to :meth:`send`.

        :raises TypeError: for unsupported inputs

        """
        with self.conn_mutex:

            self.ensure_open()

            # Unfragmented message -- this case must be handled first because
            # strings and bytes-like objects are iterable.

            if isinstance(message, str):
                self.connection.send_text(message.encode("utf-8"))
                self.send_data()

            elif isinstance(message, BytesLike):
                self.connection.send_binary(message)
                self.send_data()

            # Catch a common mistake -- passing a dict to send().

            elif isinstance(message, Mapping):
                raise TypeError("data is a dict-like object")

            # Fragmented message -- regular iterator.

            elif isinstance(message, Iterable):
                chunks = iter(message)
                try:
                    chunk = next(chunks)
                except StopIteration:
                    return

                try:
                    # First fragment.
                    if isinstance(chunk, str):
                        text = True
                        self.connection.send_text(chunk.encode("utf-8"), fin=False)
                        self.send_data()
                    elif isinstance(chunk, BytesLike):
                        text = False
                        self.connection.send_binary(chunk, fin=False)
                        self.send_data()
                    else:
                        raise TypeError("data iterable must contain bytes or str")

                    # Other fragments
                    for chunk in chunks:
                        if isinstance(chunk, str) and text:
                            self.connection.send_continuation(
                                chunk.encode("utf-8"), fin=False
                            )
                            self.send_data()
                        elif isinstance(chunk, BytesLike) and not text:
                            self.connection.send_continuation(chunk, fin=False)
                            self.send_data()
                        else:
                            raise TypeError("data iterable must contain uniform types")

                    # Final fragment.
                    self.connection.send_continuation(b"", fin=True)
                    self.send_data()

                except Exception:
                    # We're half-way through a fragmented message and we can't
                    # complete it. This makes the connection unusable.
                    self.connection.fail(1011, "error in fragmented message")
                    self.send_data()
                    raise

            else:
                raise TypeError("data must be bytes, str, or iterable")

    def close(self, code: int = 1000, reason: str = "") -> None:
        """
        Perform the closing handshake.

        :meth:`close` waits for the other end to complete the handshake and
        for the TCP connection to terminate.

        :meth:`close` is idempotent: it doesn't do anything once the
        connection is closed.

        :param code: WebSocket close code
        :param reason: WebSocket close reason

        """
        with self.conn_mutex:
            if self.connection.state is OPEN:
                self.connection.send_close(code, reason)
                self.send_data()

    def ping(self, data: Optional[Data] = None) -> threading.Event:
        """
        Send a ping.

        Return an :class:`~threading.Event` that will be set when the
        corresponding pong is received. You can ignore it if you don't intend
        to wait.

        A ping may serve as a keepalive or as a check that the remote endpoint
        received all messages up to this point::

            pong_event = ws.ping()
            pong_event.wait()  # only if you want to wait for the pong

        By default, the ping contains four random bytes. This payload may be
        overridden with the optional ``data`` argument which must be a string
        (which will be encoded to UTF-8) or a bytes-like object.

        """
        with self.conn_mutex:

            self.ensure_open()

            if data is not None:
                data = prepare_ctrl(data)

            # Protect against duplicates if a payload is explicitly set.
            if data in self.pings:
                raise ValueError("already waiting for a pong with the same data")

            # Generate a unique random payload otherwise.
            while data is None or data in self.pings:
                data = struct.pack("!I", random.getrandbits(32))

            self.pings[data] = threading.Event()

            self.connection.send_ping(data)
            self.send_data()

            return self.pings[data]

    def pong(self, data: Data = b"") -> None:
        """
        Send a pong.

        An unsolicited pong may serve as a unidirectional heartbeat.

        The payload may be set with the optional ``data`` argument which must
        be a string (which will be encoded to UTF-8) or a bytes-like object.

        """
        with self.conn_mutex:

            self.ensure_open()

            data = prepare_ctrl(data)

            self.connection.send_pong(data)
            self.send_data()

    # Private methods

    def process_event(self, event: Event) -> None:
        """
        Process one incoming event.

        """
        assert isinstance(event, Frame)
        self.messages.put(event)

    def recv_events(self) -> None:
        """
        Read incoming data from the socket and process events.

        Run this method in a thread as long as the connection is alive.

        """
        while True:
            data = self.sock.recv(65536)
            if data:
                with self.conn_mutex:
                    self.connection.receive_data(data)
                    self.send_data()
                    events = self.connection.events_received()
                # Unlock conn_mutex before processing events. Else, the
                # application can't send messages in response to events.
                for event in events:
                    self.process_event(event)
            else:
                with self.conn_mutex:
                    self.connection.receive_eof()
                    self.send_data()
                    self.messages.close()
                    self.sock.close()
                    assert not self.connection.events_received()
                    break

    def ensure_open(self) -> None:
        """
        Raise an exception if the connection isn't in the OPEN state.

        """
        assert self.conn_mutex.locked()
        if self.connection.state is not OPEN:
            self.recv_events_thread.join(self.close_timeout)
            raise self.connection.close_exc

    def send_data(self) -> None:
        """
        Write outgoing data to the socket.

        Call this method after every call to ``self.connection.send_*()``.

        """
        assert self.conn_mutex.locked()
        for data in self.connection.data_to_send():
            if data:
                self.sock.sendall(data)
            else:
                self.sock.shutdown(socket.SHUT_WR)
        if self.connection.close_expected():
            if self.tcp_close_lock.acquire(blocking=False):
                self.tcp_close_thread.start()

    def tcp_close(self) -> None:
        """
        Close the TCP connection.

        See 7.1.7 of RFC 6455.

        """
        # The TCP connection should be closed first by the server. Therefore,
        # the following sequence is expected.

        # 1. The server starts the TCP closing handshake with a FIN packet.
        #    This happens after send_eof(), when data_to_send() signals EOF
        #    with an empty bytestring, triggering shutdown(SHUT_WR).

        # 2. The client reads until reaching EOF, then completes the TCP
        #    closing handshake with a FIN packet. Again, this happens after
        #    send_eof(), when data_to_send() signals EOF with an empty
        #    bytestring, triggering shutdown(SHUT_WR) then close(). Going
        #    straight to close() would have the same effect.

        # 3. The server reads until reaching EOF, then calls close(). This
        #    doesn't send a packet.

        # Regardless of which side calls this methed, the expected behavior is
        # the same: wait until recv_events() reaches EOF then call close().

        # recv_events() could get stuck if the network is timing out. If the
        # wait times out, call close().

        self.recv_events_thread.join(self.close_timeout)
        self.sock.close()
