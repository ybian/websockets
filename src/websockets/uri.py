from __future__ import annotations

import dataclasses
import urllib.parse
from typing import Optional, Tuple

from . import exceptions


__all__ = [
    "parse_uri", "WebSocketURI",
    "parse_proxy_uri", "ProxyURI",
]


@dataclasses.dataclass
class WebSocketURI:
    """
    WebSocket URI.

    Attributes:
        secure: :obj:`True` for a ``wss`` URI, :obj:`False` for a ``ws`` URI.
        host: Normalized to lower case.
        port: Always set even if it's the default.
        path: May be empty.
        query: May be empty if the URI doesn't include a query component.
        username: Available when the URI contains `User Information`_.
        password: Available when the URI contains `User Information`_.

    .. _User Information: https://www.rfc-editor.org/rfc/rfc3986.html#section-3.2.1

    """

    secure: bool
    host: str
    port: int
    path: str
    query: str
    username: Optional[str]
    password: Optional[str]

    @property
    def resource_name(self) -> str:
        if self.path:
            resource_name = self.path
        else:
            resource_name = "/"
        if self.query:
            resource_name += "?" + self.query
        return resource_name

    @property
    def user_info(self) -> Optional[Tuple[str, str]]:
        if self.username is None:
            return None
        assert self.password is not None
        return (self.username, self.password)


# All characters from the gen-delims and sub-delims sets in RFC 3987.
DELIMS = ":/?#[]@!$&'()*+,;="


def parse_uri(uri: str) -> WebSocketURI:
    """
    Parse and validate a WebSocket URI.

    Args:
        uri: WebSocket URI.

    Returns:
        WebSocketURI: Parsed WebSocket URI.

    Raises:
        InvalidURI: if ``uri`` isn't a valid WebSocket URI.

    """
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme not in ["ws", "wss"]:
        raise exceptions.InvalidURI(uri, "scheme isn't ws or wss")
    if parsed.hostname is None:
        raise exceptions.InvalidURI(uri, "hostname isn't provided")
    if parsed.fragment != "":
        raise exceptions.InvalidURI(uri, "fragment identifier is meaningless")

    secure = parsed.scheme == "wss"
    host = parsed.hostname
    port = parsed.port or (443 if secure else 80)
    path = parsed.path
    query = parsed.query
    username = parsed.username
    password = parsed.password
    # urllib.parse.urlparse accepts URLs with a username but without a
    # password. This doesn't make sense for HTTP Basic Auth credentials.
    if username is not None and password is None:
        raise exceptions.InvalidURI(uri, "username provided without password")

    try:
        uri.encode("ascii")
    except UnicodeEncodeError:
        # Input contains non-ASCII characters.
        # It must be an IRI. Convert it to a URI.
        host = host.encode("idna").decode()
        path = urllib.parse.quote(path, safe=DELIMS)
        query = urllib.parse.quote(query, safe=DELIMS)
        if username is not None:
            assert password is not None
            username = urllib.parse.quote(username, safe=DELIMS)
            password = urllib.parse.quote(password, safe=DELIMS)

    return WebSocketURI(secure, host, port, path, query, username, password)

class ProxyURI(NamedTuple):
    """
    Proxy URI.

    :param bool secure: tells whether to connect to the proxy with TLS
    :param str host: lower-case host
    :param int port: port, always set even if it's the default
    :param str user_info: ``(username, password)`` tuple when the URI contains
      `User Information`_, else ``None``.

    .. _User Information: https://tools.ietf.org/html/rfc3986#section-3.2.1
    """

    secure: bool
    host: str
    port: int
    user_info: Optional[Tuple[str, str]]

# Work around https://bugs.python.org/issue19931

ProxyURI.secure.__doc__ = ""
ProxyURI.host.__doc__ = ""
ProxyURI.port.__doc__ = ""
ProxyURI.user_info.__doc__ = ""

def parse_proxy_uri(uri: str) -> ProxyURI:
    """
    Parse and validate an HTTP proxy URI.

    :raises ValueError: if ``uri`` isn't a valid HTTP proxy URI.

    """
    parsed = urllib.parse.urlparse(uri)
    try:
        assert parsed.scheme in ['http', 'https']
        assert parsed.hostname is not None
        assert parsed.path == '' or parsed.path == '/'
        assert parsed.params == ''
        assert parsed.query == ''
        assert parsed.fragment == ''
    except AssertionError as exc:
        raise InvalidURI(uri) from exc

    secure = parsed.scheme == 'https'
    host = parsed.hostname
    port = parsed.port or (443 if secure else 80)
    user_info = None
    if parsed.username is not None:
        # urllib.parse.urlparse accepts URLs with a username but without a
        # password. This doesn't make sense for HTTP Basic Auth credentials.
        if parsed.password is None:
            raise InvalidURI(uri)
        user_info = (parsed.username, parsed.password)
    return ProxyURI(secure, host, port, user_info)
