#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""离线自测：登录中继（无头登录流程）核心链路，不依赖华为端点与真实浏览器。

覆盖：
1. auth._finalize_login：回调校验、取消场景，及换 token 收尾（monkeypatch 华为调用）
2. 中继链路：口令门禁、/ 入口跳转、/status、/cb 回调转发到等待器、LOGIN_OK 检测、/finish

用法：.venv/bin/python test_login_relay.py
"""

from __future__ import annotations

import socket
import tempfile
import threading
import time
import traceback
from pathlib import Path

import httpx

import deveco2api.auth as A
from deveco2api import login_relay as LR
from deveco2api.config import Config


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def main() -> int:
    cfg = Config()

    # ---- 1) _finalize_login（monkeypatch 掉华为调用） ----
    A._exchange_temp_token = lambda base, tt, app: "h.p.s"  # 假 jwt（3 段）
    A._check_jwt_token = lambda base, jwt: {
        "userInfo": {"accessToken": "AT", "refreshToken": "RT", "userId": "U1", "name": "N"}
    }
    A._parse_jwt = lambda t: {"userId": "U1", "userName": "N"}

    res = A._finalize_login(cfg, {"code": "x", "tempToken": "tt1", "siteId": "1", "quit": ""})
    assert res.access_token == "AT" and res.user_id == "U1" and res.jwt_token == "h.p.s", res
    try:
        A._finalize_login(cfg, {"code": "x", "tempToken": "tt", "siteId": "1", "quit": "access_denied"})
        raise AssertionError("取消授权应抛错")
    except RuntimeError:
        pass
    try:
        A._finalize_login(cfg, {"code": "x", "tempToken": "tt", "siteId": "2", "quit": ""})
        raise AssertionError("非中国区应抛错")
    except RuntimeError:
        pass
    print("[+] _finalize_login：正常收尾 / 取消 / 非中国区 场景均正确")

    # 隧道 URL 解析：必须排除日志里的 api.trycloudflare.com 干扰项
    assert LR._TUNNEL_URL_RE.search("|  https://struggle-huntington-lowest.trycloudflare.com  |")
    assert not LR._TUNNEL_URL_RE.search("requesting on https://api.trycloudflare.com/v2/tunnels")
    print("[+] 隧道 URL 解析：真实地址命中 / api 干扰项排除")

    # ---- 2) 中继链路（真实等待器 + 假回调，无华为调用） ----
    state = Path(tempfile.mkdtemp(prefix="login-relay-test-"))
    relay_port = _free_port()
    waiter_port = _free_port()
    key = "test-key"
    nonce = "testnonce123"

    server, actual_waiter_port = A._start_callback_server(waiter_port, nonce)
    LR.configure(port=relay_port, access_key=key, nonce=nonce, waiter_port=actual_waiter_port,
                 session_file=str(state / "relay_session.json"), waiter_log=str(state / "deveco-login.log"))
    (state / "deveco-login.log").write_text("", encoding="utf-8")
    LR.S.reset()
    srv = LR.create_server()
    threading.Thread(target=srv.run, daemon=True).start()

    base = f"http://127.0.0.1:{relay_port}"
    ready = False
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            if httpx.get(f"{base}/status", params={"k": key}, timeout=2).status_code == 200:
                ready = True
                break
        except Exception:
            pass
        time.sleep(0.2)
    assert ready, "中继服务未就绪"

    try:
        with httpx.Client(follow_redirects=False) as c:
            # 口令门禁
            r = c.get(f"{base}/status")
            assert r.status_code == 403, r.status_code
            # 入口跳转（带口令；浏览器会记住 rk cookie；RedirectResponse 默认 307）
            r = c.get(f"{base}/", params={"k": key})
            assert r.status_code in (302, 307) and "authrouter/forward" in r.headers.get("location", ""), dict(r.headers)
            # LOGIN_OK 检测
            assert LR._waiter_done() is False
            with open(state / "deveco-login.log", "a", encoding="utf-8") as f:
                f.write("LOGIN_OK\n")
            assert LR._waiter_done() is True
            # 回调经中继转发 → 等待器收到（浏览器侧凭 rk cookie 通过门禁）
            r = c.post(f"{base}/cb/{actual_waiter_port}/callback",
                       params={"code": nonce, "tempToken": "tk1", "siteId": "1"})
            assert r.status_code == 303, r.status_code
            deadline = time.time() + 5
            while time.time() < deadline and A._CallbackHandler.result is None:
                time.sleep(0.1)
            assert A._CallbackHandler.result is not None, "等待器未收到回调"
            assert A._CallbackHandler.result["tempToken"] == "tk1", A._CallbackHandler.result
            assert A._callback_event.is_set()
            # finish 页面可访问
            r = c.get(f"{base}/finish")
            assert r.status_code == 200 and "DevEco" in r.text
        print("[+] 中继链路：门禁 / 入口跳转 / 回调转发 / LOGIN_OK 检测 / finish 全部通过")
    finally:
        srv.should_exit = True
        try:
            server.shutdown()
        except Exception:
            pass
        time.sleep(0.5)

    print("\nALL PASS ✔")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        print("\nTEST FAILED ✘")
        raise SystemExit(1)
