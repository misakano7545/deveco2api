#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""离线自测：用本地 mock 上游验证 deveco2api 核心链路（不需要华为账号）。

覆盖本次修复点：
1. access_token 探测兼容 modelConfig 的 success:true / code:200 两种响应形态
2. 工具调用字段（tool_calls / tool_call_id / tools）不再被截断
3. access_token 失效时自动用 jwtToken 刷新并重试（401 → refresh → retry）
4. 流式 / 非流式转发、请求头（lang、Chat-Id、x-deveco-*）、/v1/models 解析
5. 思维链剥离：GLM-5.3（含 </think> 跨 chunk）转入 reasoning_content；
   未思考/截断整段按正文兜底；非思考模型不受影响
6. 上游忙/报错统一转译（HTTP 4xx + error 对象、200 + 空 data: 帧 + error 帧、
   200 + error 对象）→ 规范 HTTP 429，空帧被跳过

用法：.venv/bin/python test_mock_upstream.py
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi.testclient import TestClient

from deveco2api.auth import _test_access_token
from deveco2api.config import Config, DevEcoAuthConfig
from deveco2api.proxy import create_app

API_KEY = "test-key"
OLD_TOKEN = "old-token"
NEW_TOKEN = "new-token"

STATE = {
    "old_valid": True,      # OLD_TOKEN 初始有效；调用前置 False 触发 401 刷新路径
    "omit_success": False,  # modelConfig 响应是否省略 success 字段
    "s53_think": True,      # GLM-5.3 流式是否输出思维链（False 模拟未思考/截断）
    "rl_stream": False,     # 流式返回上游限流形态（空 data: 帧 + error 帧）
    "rl_nostream": None,    # 非流式限流形态：None / "soft"（200+error 对象）/ "hard"（HTTP 429+error）
}
REQS: list[dict] = []

MODEL_CONFIG = {
    "code": 200,
    "body": {
        "version": 1,
        "inner_models": [
            {
                "protocol": "openai",
                "group_name": "GLM",
                "group_name_cn": "智谱",
                "model_configs": [
                    {
                        "id": 1,
                        "model_id": "GLM-5.1",
                        "context_window": 32768,
                        "output": 8192,
                        "thinking_mode": "on",
                        "tool_call_mode": "tool_calls",
                    },
                    {
                        "id": 2,
                        "model_id": "GLM-5.3",
                        "context_window": 32768,
                        "output": 8192,
                        "thinking_mode": "on",
                        "tool_call_mode": "tool_calls",
                    },
                ],
                "task_default_model_map": {"blacklist": ""},
            }
        ],
    },
}


class MockUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"  # SSE 用连接关闭定界

    RATE_LIMIT_ERROR = {
        "message": "New session request rate exceeded. Please retry later or set up a custom model.",
        "type": "UserSessionLimitExceeded",
        "code": "403",
    }

    def log_message(self, *args):  # 静默
        pass

    # ---- helpers ----
    def _json(self, code: int, obj: dict) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        n = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(n).decode("utf-8", "ignore") if n else ""
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return {}

    def _token_ok(self) -> bool:
        auth = self.headers.get("Authorization", "")
        token = auth[len("Bearer "):] if auth.startswith("Bearer ") else auth
        return token == "valid-tok" or token == NEW_TOKEN or (token == OLD_TOKEN and STATE["old_valid"])

    def _sse(self, model: str = "") -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: \n\n")  # 上游偶发空帧：代理必须跳过
        if model == "GLM-5.3" and STATE["s53_think"]:
            # 思考模型形态：思维链裸写进正文，且 </think> 被拆到两个 chunk
            parts = ["Let me think", " about it", " carefully.</thi", "nk>", "42"]
        else:
            parts = ["po", "ng"]
        for i, part in enumerate(parts):
            delta = {"role": "assistant", "content": part} if i == 0 else {"content": part}
            ch = {
                "id": "c1",
                "object": "chat.completion.chunk",
                "choices": [
                    {
                        "index": 0,
                        "delta": delta,
                        "finish_reason": "stop" if i == len(parts) - 1 else None,
                    }
                ],
            }
            self.wfile.write(("data: %s\n\n" % json.dumps(ch)).encode())
            self.wfile.flush()
        self.wfile.write(b"data: [DONE]\n\n")

    def _sse_rate_limit(self) -> None:
        """上游限流形态：HTTP 200 + 空 data: 帧 + error 帧，无 [DONE]。"""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: \n\n")
        self.wfile.write(("data: %s\n\n" % json.dumps({"error": dict(self.RATE_LIMIT_ERROR)})).encode())

    # ---- routes ----
    def do_GET(self):  # noqa: N802
        if self.path.startswith("/codeGenie/modelConfig"):
            REQS.append({"kind": "modelConfig", "auth": self.headers.get("Authorization", "")})
            if not self._token_ok():
                self._json(200, {"errorCode": 5002, "errorMsg": "Request parameter error. authorization is null."})
                return
            payload = dict(MODEL_CONFIG)
            if not STATE["omit_success"]:
                payload["success"] = True
            self._json(200, payload)
        elif self.path.startswith("/authrouter/auth/api/jwToken/check"):
            REQS.append({"kind": "refresh", "refresh": self.headers.get("refresh", "")})
            self._json(200, {
                "status": True,
                "userInfo": {
                    "accessToken": NEW_TOKEN,
                    "refreshToken": "refresh-2",
                    "userId": "u1",
                    "name": "tester",
                    "realName": True,
                    "nationalCode": "CN",
                },
            })
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        body = self._body()
        if self.path == "/sse/codeGenie/maas/v2/no-stream/chat/completions":
            REQS.append({"kind": "chat_no_stream", "auth": self.headers.get("Authorization", ""),
                         "headers": {k.lower(): v for k, v in self.headers.items()}, "body": body})
            if not self._token_ok():
                self._json(401, {"error": "token expired"})
                return
            if STATE["rl_nostream"] == "soft":
                self._json(200, {"error": dict(self.RATE_LIMIT_ERROR)})
                return
            if STATE["rl_nostream"] == "hard":
                self._json(429, {"error": dict(self.RATE_LIMIT_ERROR)})
                return
            content = (
                "Let me think about it carefully.</think>42"
                if body.get("model") == "GLM-5.3"
                else "pong"
            )
            self._json(200, {
                "id": "c1",
                "object": "chat.completion",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            })
        elif self.path == "/sse/codeGenie/maas/v2/chat/completions":
            REQS.append({"kind": "chat_stream", "auth": self.headers.get("Authorization", "")})
            if not self._token_ok():
                self._json(401, {"error": "token expired"})
                return
            if STATE["rl_stream"]:
                self._sse_rate_limit()
                return
            self._sse(body.get("model", ""))
        else:
            self._json(404, {"error": "not found"})


def main() -> int:
    server = ThreadingHTTPServer(("127.0.0.1", 0), MockUpstream)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    print(f"[*] mock upstream: {base}")

    # ---- 1) access_token 探测的两种响应形态 ----
    STATE["omit_success"] = False
    assert _test_access_token(base, "valid-tok") is True, "success:true 形态应判定有效"
    STATE["omit_success"] = True
    assert _test_access_token(base, "valid-tok") is True, "仅 code:200 形态也应判定有效（本次修复点）"
    STATE["omit_success"] = False
    assert _test_access_token(base, "bad-tok") is False, "无效 token 应判定失效"
    print("[+] access_token 探测：success:true / code:200 均可，bad token 正确判负")

    # ---- 2) 端到端：/v1/models + 非流式 + 401 自动刷新 + 流式 ----
    config = Config()
    config.server.api_key = API_KEY
    config.deveco.base_url = base
    config.deveco.auth = DevEcoAuthConfig(
        jwt_token="jwt-1", access_token=OLD_TOKEN, refresh_token="r1", user_id="u1", user_name="tester"
    )
    tmpdir = Path(tempfile.mkdtemp(prefix="deveco2api-test-"))
    app = create_app(config, str(tmpdir / "config.toml"))
    hdr = {"Authorization": f"Bearer {API_KEY}"}

    with TestClient(app) as client:
        # /v1/models
        r = client.get("/v1/models", headers=hdr)
        assert r.status_code == 200, r.text
        ids = [m["id"] for m in r.json()["data"]]
        assert "GLM-5.1" in ids and "GLM-5.3" in ids, ids
        print(f"[+] /v1/models -> {ids}")

        # 无 key 拒绝
        assert client.get("/v1/models").status_code == 401
        print("[+] 无 API key 正确返回 401")

        # 模拟 access_token 过期：下一次调用应先被 401 拒绝，再自动刷新重试
        STATE["old_valid"] = False

        # 非流式 + 工具调用历史（先 401 → 刷新 → 重试）
        payload = {
            "model": "GLM-5.1",
            "stream": False,
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": None, "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "get_time", "arguments": "{}"}}
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "12:00"},
            ],
            "tools": [{"type": "function", "function": {"name": "get_time", "parameters": {"type": "object"}}}],
        }
        r = client.post("/v1/chat/completions", headers=hdr, json=payload)
        assert r.status_code == 200, r.text
        assert r.json()["choices"][0]["message"]["content"] == "pong"
        chat_reqs = [q for q in REQS if q["kind"] == "chat_no_stream"]
        assert len(chat_reqs) == 2, f"应有一次 401 重试，实际 {len(chat_reqs)} 次"
        assert chat_reqs[0]["auth"] == f"Bearer {OLD_TOKEN}"
        assert chat_reqs[1]["auth"] == f"Bearer {NEW_TOKEN}", "重试应使用刷新后的 token"
        assert any(q["kind"] == "refresh" and q["refresh"] == "true" for q in REQS), "应发生 refresh=true 刷新"
        sent = chat_reqs[1]["body"]
        assert sent["messages"][1]["tool_calls"][0]["id"] == "call_1", "tool_calls 必须透传"
        assert sent["messages"][2]["tool_call_id"] == "call_1", "tool_call_id 必须透传"
        assert sent["tools"][0]["function"]["name"] == "get_time"
        h = chat_reqs[1]["headers"]
        assert h.get("lang") == "en"
        assert h.get("x-deveco-client") == "cli"
        assert h.get("session-id") and h.get("session-id") == h.get("x-deveco-session")
        assert (h.get("user-agent") or "").startswith("deveco/")
        assert h.get("chat-id"), "Chat-Id 必须存在"
        print("[+] 非流式：401→刷新→重试成功；tool_calls/tool_call_id 透传；头信息齐全")

        # 流式
        r = client.post("/v1/chat/completions", headers=hdr, json={**payload, "stream": True})
        assert r.status_code == 200, r.text
        text = r.text
        assert "data: [DONE]" in text, text[:300]
        assert "reasoning_content" not in text, "非思考模型不应有 reasoning_content"
        content = ""
        for line in text.splitlines():
            if line.startswith("data: ") and line[6:] != "[DONE]":
                content += json.loads(line[6:])["choices"][0]["delta"].get("content", "")
        assert content == "pong", content
        stream_reqs = [q for q in REQS if q["kind"] == "chat_stream"]
        assert len(stream_reqs) == 1 and stream_reqs[0]["auth"] == f"Bearer {NEW_TOKEN}"
        print("[+] 流式：SSE 透传 + [DONE]（上游空帧已剔除），内容拼装正确")

        # 运行期刷新后的 token 已写回配置文件
        cfg_text = (tmpdir / "config.toml").read_text(encoding="utf-8")
        assert NEW_TOKEN in cfg_text, "刷新后的 access_token 应写回 config.toml"
        print("[+] 刷新后的 access_token 已写回配置文件")

        # ---- 3) /v1/models 自动刷新（失效 token → 200+errorCode → 刷新 → 重试） ----
        config.deveco.auth.access_token = OLD_TOKEN  # 人为把内存 token 置回已失效值
        before = len([q for q in REQS if q["kind"] == "modelConfig"])
        r = client.get("/v1/models", headers=hdr)
        assert r.status_code == 200, r.text
        assert "GLM-5.1" in [m["id"] for m in r.json()["data"]]
        mc = [q for q in REQS if q["kind"] == "modelConfig"][before:]
        assert len(mc) == 2, f"应为 失败+重试 两次，实际 {len(mc)}"
        assert mc[0]["auth"] == f"Bearer {OLD_TOKEN}" and mc[1]["auth"] == f"Bearer {NEW_TOKEN}"
        print("[+] /v1/models：失效 token（200+errorCode）→ 自动刷新 → 重试成功")

        # ---- 4) 思维链剥离：GLM-5.3（流式 + 非流式） ----
        think = "Let me think about it carefully."

        r = client.post("/v1/chat/completions", headers=hdr,
                        json={"model": "GLM-5.3", "messages": [{"role": "user", "content": "1+1?"}]})
        assert r.status_code == 200, r.text
        msg = r.json()["choices"][0]["message"]
        assert msg["content"] == "42", msg
        assert msg["reasoning_content"] == think, msg
        print("[+] 非流式 GLM-5.3：思维链 → reasoning_content，正文 = '42'")

        def _stream_parts(model: str) -> tuple[str, str]:
            resp = client.post("/v1/chat/completions", headers=hdr,
                               json={"model": model, "stream": True,
                                     "messages": [{"role": "user", "content": "1+1?"}]})
            assert resp.status_code == 200, resp.text
            got_content, got_reasoning = "", ""
            for line in resp.text.splitlines():
                if line.startswith("data: ") and line[6:] != "[DONE]":
                    delta = json.loads(line[6:])["choices"][0]["delta"]
                    got_content += delta.get("content", "")
                    got_reasoning += delta.get("reasoning_content", "")
            return got_content, got_reasoning

        content, reasoning = _stream_parts("GLM-5.3")
        assert content == "42", content          # </think> 跨 chunk 也不泄漏
        assert reasoning == think, reasoning
        print("[+] 流式 GLM-5.3：</think> 跨 chunk，思维链转入 reasoning_content，正文 = '42'")

        STATE["s53_think"] = False  # 模拟模型未思考/被截断：整段按正文兜底
        content, reasoning = _stream_parts("GLM-5.3")
        assert content == "pong" and reasoning == "", (content, reasoning)
        STATE["s53_think"] = True
        print("[+] 流式 GLM-5.3 无 </think>：整段按正文兜底，不丢内容")

        content, reasoning = _stream_parts("GLM-5.1")
        assert content == "pong" and reasoning == "", (content, reasoning)
        print("[+] 流式 GLM-5.1：不受影响，无 reasoning_content")

        # ---- 5) 上游限流/错误 → 规范 429（三种姿势） ----
        STATE["rl_stream"] = True
        resp = client.post("/v1/chat/completions", headers=hdr,
                           json={"model": "GLM-5.1", "stream": True, "messages": [{"role": "user", "content": "hi"}]})
        assert resp.status_code == 429, resp.text
        err = resp.json()["error"]
        assert err["type"] == "UserSessionLimitExceeded", err
        assert err["message"].startswith("New session request rate exceeded"), err
        STATE["rl_stream"] = False
        print("[+] 流式上游限流帧（200 + 空帧 + error 帧）→ HTTP 429 + 原始 error 信息")

        STATE["rl_nostream"] = "hard"  # 上游 HTTP 4xx + error 对象（实测形态）
        resp = client.post("/v1/chat/completions", headers=hdr,
                           json={"model": "GLM-5.1", "messages": [{"role": "user", "content": "hi"}]})
        assert resp.status_code == 429, resp.text
        assert resp.json()["error"]["type"] == "UserSessionLimitExceeded", resp.text
        STATE["rl_nostream"] = "soft"  # 上游 200 + error 对象（防御形态）
        resp = client.post("/v1/chat/completions", headers=hdr,
                           json={"model": "GLM-5.1", "messages": [{"role": "user", "content": "hi"}]})
        assert resp.status_code == 429, resp.text
        assert resp.json()["error"]["type"] == "UserSessionLimitExceeded", resp.text
        STATE["rl_nostream"] = None
        print("[+] 非流式上游错误（HTTP 4xx / 200+error 对象）→ HTTP 429")

    # ---- 6) token 保活：keepalive_hours 定时循环自动刷新 ----
    cfg2 = Config()
    cfg2.server.api_key = API_KEY
    cfg2.deveco.base_url = base
    cfg2.deveco.keepalive_hours = 0.0005  # ≈1.8s，便于实测
    cfg2.deveco.auth = DevEcoAuthConfig(jwt_token="jwt-1", access_token=NEW_TOKEN)
    tmpdir2 = Path(tempfile.mkdtemp(prefix="deveco2api-keepalive-"))
    app2 = create_app(cfg2, str(tmpdir2 / "config.toml"))
    n0 = len([q for q in REQS if q["kind"] == "refresh"])
    with TestClient(app2) as client2:
        deadline = time.time() + 8
        while time.time() < deadline:
            if len([q for q in REQS if q["kind"] == "refresh"]) - n0 >= 2:
                break
            time.sleep(0.4)
            client2.get("/health")
    n1 = len([q for q in REQS if q["kind"] == "refresh"])
    assert n1 - n0 >= 2, f"保活应至少触发 2 次刷新（间隔 ≈1.8s），实际 {n1 - n0} 次"
    print(f"[+] token 保活：{n1 - n0} 次定时刷新（keepalive_hours=0.0005）")

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
