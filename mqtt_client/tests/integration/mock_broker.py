#!/usr/bin/env python3
"""极简 MQTT v5 broker —— 仅用于 mqtt_client 的端到端冒烟测试。

支持：CONNECT / SUBSCRIBE / UNSUBSCRIBE / PUBLISH(QoS0/1) / PINGREQ / DISCONNECT，
并按键值（topic filter 匹配规则）把消息转发给所有匹配的订阅者。

用法：
    python3 mock_broker.py [port]        # 默认 18830

仅依赖标准库，禁止用于生产环境。
"""

from __future__ import annotations

import socket
import sys
import threading

HOST = "127.0.0.1"


# ---------------------------------------------------------------------------
# 编解码工具
# ---------------------------------------------------------------------------

def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("连接已关闭")
        buf += chunk
    return buf


def read_varint(sock: socket.socket) -> int:
    value = 0
    shift = 0
    while True:
        b = recv_exact(sock, 1)[0]
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            return value
        shift += 7


def encode_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def encode_utf8(s: str) -> bytes:
    raw = s.encode("utf-8")
    return len(raw).to_bytes(2, "big") + raw


class Reader:
    """按 MQTT 数据类型顺序读取一个报文体"""

    def __init__(self, buf: bytes) -> None:
        self.buf = buf
        self.pos = 0

    def u8(self) -> int:
        v = self.buf[self.pos]
        self.pos += 1
        return v

    def u16(self) -> int:
        v = int.from_bytes(self.buf[self.pos:self.pos + 2], "big")
        self.pos += 2
        return v

    def utf8(self) -> str:
        n = self.u16()
        v = self.buf[self.pos:self.pos + n].decode("utf-8", "replace")
        self.pos += n
        return v

    def binary(self) -> bytes:
        n = self.u16()
        v = self.buf[self.pos:self.pos + n]
        self.pos += n
        return v

    def varint(self) -> int:
        value = 0
        shift = 0
        while True:
            b = self.u8()
            value |= (b & 0x7F) << shift
            if not (b & 0x80):
                return value
            shift += 7

    def skip_properties(self) -> None:
        n = self.varint()
        self.pos += n

    def rest(self) -> bytes:
        v = self.buf[self.pos:]
        self.pos = len(self.buf)
        return v

    def done(self) -> bool:
        return self.pos >= len(self.buf)


# ---------------------------------------------------------------------------
# topic filter 匹配（与 mqtt_client/topic_filter.hpp 规则一致）
# ---------------------------------------------------------------------------

def topic_matches_filter(topic: str, flt: str) -> bool:
    if topic.startswith("$") and flt[:1] in ("+", "#"):
        return False

    t = topic.split("/")
    f = flt.split("/")

    for i, seg in enumerate(f):
        if seg == "#":
            return i == len(f) - 1
        if i >= len(t):
            # topic 已结束、filter 还有级：只有 'a/#' 形式能匹配 'a'
            return f[i:] == ["#"]
        if seg != "+" and seg != t[i]:
            return False

    return len(t) == len(f)


# ---------------------------------------------------------------------------
# broker
# ---------------------------------------------------------------------------

BROADCAST_LOCK = threading.Lock()
SESSIONS: list["Session"] = []


def broadcast(topic: str, payload: bytes) -> None:
    with BROADCAST_LOCK:
        targets = list(SESSIONS)
    for s in targets:
        s.deliver(topic, payload)


class Session:
    def __init__(self, sock: socket.socket, addr) -> None:
        self.sock = sock
        self.addr = addr
        self.client_id = ""
        self.subs: dict[str, int] = {}
        self.send_lock = threading.Lock()

    # -- 发送 --------------------------------------------------------------

    def send(self, data: bytes) -> None:
        with self.send_lock:
            self.sock.sendall(data)

    def deliver(self, topic: str, payload: bytes) -> None:
        matches = [(f, q) for f, q in self.subs.items() if topic_matches_filter(topic, f)]
        if not matches:
            return
        qos = max(q for _, q in matches)
        body = encode_utf8(topic) + (b"\x00\x01" if qos else b"") + b"\x00" + payload
        self.send(bytes([0x30 | (qos << 1)]) + encode_varint(len(body)) + body)

    # -- 报文处理 ----------------------------------------------------------

    def on_connect(self, r: Reader) -> None:
        proto = r.utf8()
        level = r.u8()
        flags = r.u8()
        r.u16()  # keep alive
        r.skip_properties()
        self.client_id = r.utf8()
        if flags & 0x04:  # will
            r.utf8()
            r.binary()
        if flags & 0x80:
            r.utf8()
        if flags & 0x40:
            r.binary()

        print(f"[broker] CONNECT     id={self.client_id!r} proto={proto} v{level}", flush=True)
        self.send(b"\x20\x03\x00\x00\x00")  # CONNACK: session_present=0, reason=0, 无属性

    def on_subscribe(self, r: Reader) -> None:
        pid = r.u16()
        r.skip_properties()
        codes = bytearray()
        while not r.done():
            flt = r.utf8()
            qos = r.u8() & 0x03
            self.subs[flt] = qos
            codes.append(qos)
            print(f"[broker] SUBSCRIBE   {flt} qos={qos}", flush=True)
        body = pid.to_bytes(2, "big") + b"\x00" + bytes(codes)
        self.send(b"\x90" + encode_varint(len(body)) + body)

    def on_unsubscribe(self, r: Reader) -> None:
        pid = r.u16()
        r.skip_properties()
        codes = bytearray()
        while not r.done():
            flt = r.utf8()
            self.subs.pop(flt, None)
            codes.append(0x00)
            print(f"[broker] UNSUBSCRIBE {flt}", flush=True)
        body = pid.to_bytes(2, "big") + b"\x00" + bytes(codes)
        self.send(b"\xb0" + encode_varint(len(body)) + body)

    def on_publish(self, flags: int, r: Reader) -> None:
        topic = r.utf8()
        qos = (flags >> 1) & 0x03
        pid = r.u16() if qos else 0
        r.skip_properties()
        payload = r.rest()
        print(f"[broker] PUBLISH     {topic} qos={qos} payload={payload!r}", flush=True)
        if qos == 1:
            self.send(b"\x40\x04" + pid.to_bytes(2, "big") + b"\x00\x00")  # PUBACK
        broadcast(topic, payload)

    def run(self) -> None:
        with BROADCAST_LOCK:
            SESSIONS.append(self)
        try:
            while True:
                first = recv_exact(self.sock, 1)[0]
                ptype = first >> 4
                flags = first & 0x0F
                body = recv_exact(self.sock, read_varint(self.sock))
                r = Reader(body)

                if ptype == 1:  # CONNECT
                    self.on_connect(r)
                elif ptype == 3:  # PUBLISH
                    self.on_publish(flags, r)
                elif ptype == 8:  # SUBSCRIBE
                    self.on_subscribe(r)
                elif ptype == 10:  # UNSUBSCRIBE
                    self.on_unsubscribe(r)
                elif ptype == 12:  # PINGREQ
                    self.send(b"\xd0\x00")
                elif ptype == 14:  # DISCONNECT
                    print(f"[broker] DISCONNECT  id={self.client_id!r}", flush=True)
                    break
        except (ConnectionError, OSError):
            pass
        finally:
            with BROADCAST_LOCK:
                if self in SESSIONS:
                    SESSIONS.remove(self)
            try:
                self.sock.close()
            except OSError:
                pass
            print(f"[broker] 断开        id={self.client_id!r}", flush=True)


def serve(port: int) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, port))
    srv.listen(8)
    print(f"[broker] listening on {HOST}:{port}", flush=True)

    while True:
        conn, addr = srv.accept()
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=Session(conn, addr).run, daemon=True).start()


if __name__ == "__main__":
    serve(int(sys.argv[1]) if len(sys.argv) > 1 else 18830)
