#!/usr/bin/env python3
"""启动 mock broker，运行端到端测试，然后收拾干净。

    python3 run_online_test.py <test_binary> <mock_broker.py>
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time

HOST = "127.0.0.1"


def free_port() -> int:
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


def main() -> int:
    if len(sys.argv) != 3:
        print("用法: run_online_test.py <test_binary> <mock_broker.py>", file=sys.stderr)
        return 2

    test_bin, broker_py = sys.argv[1], sys.argv[2]
    port = free_port()

    broker = subprocess.Popen(
        [sys.executable, broker_py, str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # 等 broker 就绪
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with socket.create_connection((HOST, port), timeout=0.2):
                    break
            except OSError:
                if broker.poll() is not None:
                    print("[runner] broker 提前退出", file=sys.stderr)
                    print(broker.stdout.read() if broker.stdout else "", file=sys.stderr)
                    return 2
                time.sleep(0.05)
        else:
            print("[runner] 等待 broker 就绪超时", file=sys.stderr)
            return 2

        proc = subprocess.run([test_bin, f"tcp://{HOST}:{port}"], text=True)
        return proc.returncode
    finally:
        broker.terminate()
        try:
            out, _ = broker.communicate(timeout=5)
            sys.stdout.write("[broker 日志]\n" + (out or ""))
        except subprocess.TimeoutExpired:
            broker.kill()
            os.waitpid(broker.pid, 0) if hasattr(os, "waitpid") else None


if __name__ == "__main__":
    sys.exit(main())
