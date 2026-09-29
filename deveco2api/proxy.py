# -*- coding: utf-8 -*-
"""OpenAI 兼容 API 代理，转发至 DevEco MaaS。"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncGenerator, Optional

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .auth import refresh_access_token_sync
from .config import Config, save_config
from .id import chat_id, message_id, session_id
from .logger import setup_logger

logger = setup_logger("deveco2api.proxy")


bearer_scheme = HTTPBearer(auto_error=False)


def _session_chat_id_map() -> dict[str, str]:
    # 单进程内按 session_id 缓存 Chat-Id
    if not hasattr(_session_chat_id_map, "cache"):
        _session_chat_id_map.cache = {}
    return _session_chat_id_map.cache


def _get_chat_id(session_id_value: str) -> str:
    cache = _session_chat_id_map()
    if session_id_value not in cache:
        cache[session_id_value] = chat_id()
    return cache[session_id_value]


def _verify_api_key(
    config: Config, credentials: Optional[HTTPAuthorizationCredentials]
) -> None:
    expected = config.server.api_key
    if not expected:
        return
    if credentials is None or credentials.credentials != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


def _build_deveco_headers(config: Config, session_id_value: str, user_msg_id: str) -> dict[str, str]:
    access_token = config.deveco.auth.access_token
    chat_id_value = _get_chat_id(session_id_value)
    headers: dict[str, str] = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Chat-Id": chat_id_value,
        "Session-Id": session_id_value,
        "x-deveco-client": config.deveco.client,
        "x-deveco-project": config.deveco.project,
        "x-deveco-request": user_msg_id,
        "x-deveco-session": session_id_value,
        "User-Agent": config.deveco.user_agent,
        "lang": "en",
        "Accept": "*/*",
    }
    return headers


def _normalize_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """保留原始 messages，仅确保 content 为字符串。"""
    normalized: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role", "user")
        content = m.get("content", "")
        if isinstance(content, list):
            # 简单将多模态内容拼接为文本；实际可按需扩展
            parts = []
            for part in content:
                if isinstance(part, dict):
                    if part.get("type") == "text":
                        parts.append(part.get("text", ""))
                    elif part.get("type") == "image_url":
                        parts.append(f"[image: {part.get('image_url', {}).get('url', '')}]")
            content = "\n".join(parts)
        item: dict[str, Any] = {"role": role, "content": content}
        # 保留工具/函数调用字段，否则多轮工具对话会被截断
        for key in ("name", "tool_calls", "tool_call_id", "function_call"):
            if key in m:
                item[key] = m[key]
        normalized.append(item)
    return normalized


THINK_END = "</think>"


def _sse_chunk(template: dict[str, Any], delta: dict[str, Any]) -> str:
    """基于上游最近一个 chunk 的信封，合成一帧 OpenAI 格式 SSE。"""
    chunk = {k: template[k] for k in ("id", "object", "created", "model") if k in template}
    chunk.setdefault("object", "chat.completion.chunk")
    chunk["choices"] = [{"index": 0, "delta": delta, "finish_reason": None}]
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"


def _strip_thinking_nonstream(payload: dict[str, Any]) -> dict[str, Any]:
    """非流式：content 中 </think> 之前是上游未剥离的思维链，拆到 reasoning_content。"""
    for choice in payload.get("choices") or []:
        message = choice.get("message") or {}
        content = message.get("content")
        if isinstance(content, str) and THINK_END in content:
            think, _, answer = content.partition(THINK_END)
            message["content"] = answer.lstrip()
            if think.strip():
                message["reasoning_content"] = think
    return payload


def _extract_sse_error(text: str) -> Optional[dict[str, Any]]:
    """上游偶尔用 HTTP 200 + SSE error 帧报错（如新会话限流）：摘出第一个 error 对象。"""
    for line in text.splitlines():
        if not line.startswith("data: "):
            continue
        try:
            obj = json.loads(line[6:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and isinstance(obj.get("error"), dict):
            return obj["error"]
    return None


def _upstream_error_status(err: dict[str, Any]) -> int:
    """把上游 error 对象映射为 HTTP 状态码（限流→429，其余按 code，兜底 502）。"""
    if err.get("type") == "UserSessionLimitExceeded":
        return 429
    try:
        code = int(err.get("code", 0))
    except (TypeError, ValueError):
        code = 0
    return code if 400 <= code <= 599 else 502


def _extract_http_error(resp: httpx.Response) -> dict[str, Any]:
    """从上游 HTTP 错误响应中取出 error 对象（取不到则兜底为 message）。"""
    try:
        payload = resp.json()
    except Exception:
        payload = None
    if isinstance(payload, dict):
        err = payload.get("error")
        if isinstance(err, dict):
            return err
        msg = payload.get("errorMsg") or payload.get("message") or payload.get("detail")
        if msg:
            return {"message": str(msg), "code": str(resp.status_code)}
    return {"message": f"Upstream HTTP {resp.status_code}: {resp.text[:300]}", "code": str(resp.status_code)}


def _build_deveco_body(config: Config, request_body: dict[str, Any]) -> dict[str, Any]:
    model = request_body.get("model", config.deveco.model)
    messages = _normalize_messages(request_body.get("messages", []))
    stream = request_body.get("stream", False)
    max_tokens = request_body.get("max_tokens", 32000)

    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": stream,
    }
    if stream:
        body["stream_options"] = {"include_usage": True}

    # 透传工具调用相关参数
    if "tools" in request_body:
        body["tools"] = request_body["tools"]
    if "tool_choice" in request_body:
        tc = request_body["tool_choice"]
        # DevEco 后端只接受字符串枚举，OpenAI 对象形式统一映射为 required
        if isinstance(tc, dict) and tc.get("type") == "function":
            body["tool_choice"] = "required"
        elif tc in ("none", "auto", "required"):
            body["tool_choice"] = tc
        else:
            body["tool_choice"] = "auto"

    # 透传其他常见参数
    for key in ("temperature", "top_p", "frequency_penalty", "presence_penalty", "stop", "seed"):
        if key in request_body:
            body[key] = request_body[key]
    return body


def _create_app(config: Config, config_path: str = "config.toml") -> FastAPI:
    app = FastAPI(title="DevEco2API", version="0.1.0")

    # 复用异步 httpx 客户端，保持连接池
    client = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0))
    # 串行化 token 刷新（保活定时器与请求 401 路径可能并发）
    refresh_lock = asyncio.Lock()

    @app.on_event("shutdown")
    async def _close_client():
        await client.aclose()

    @app.get("/v1/models")
    async def list_models(credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme)):
        _verify_api_key(config, credentials)
        url = f"{config.deveco.base_url.rstrip('/')}/codeGenie/modelConfig"
        params = {"localVersion": "0", "pluginVersion": "CLI.0.2.0"}
        headers = {
            "Authorization": f"Bearer {config.deveco.auth.access_token}",
            "Content-Type": "application/json",
            "User-Agent": config.deveco.user_agent,
            "Accept": "*/*",
        }
        async def _fetch_model_config() -> dict:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp.json()

        try:
            data = await _fetch_model_config()
        except httpx.HTTPStatusError as e:
            if e.response.status_code not in (401, 403) or not await _try_refresh_token():
                logger.error("获取模型列表失败: %s", e)
                raise HTTPException(status_code=502, detail=f"Upstream error: {e}")
            headers["Authorization"] = f"Bearer {config.deveco.auth.access_token}"
            try:
                data = await _fetch_model_config()
            except httpx.HTTPError as e2:
                logger.error("刷新后获取模型列表失败: %s", e2)
                raise HTTPException(status_code=502, detail=f"Upstream error: {e2}")
        except httpx.HTTPError as e:
            logger.error("获取模型列表失败: %s", e)
            raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

        # token 失效时上游可能返回 200 + errorCode（不抛 HTTP 错误）：刷新后重试一次
        if not (data.get("success") is True or data.get("code") == 200):
            if await _try_refresh_token():
                headers["Authorization"] = f"Bearer {config.deveco.auth.access_token}"
                try:
                    data = await _fetch_model_config()
                except httpx.HTTPError as e2:
                    logger.error("刷新后获取模型列表失败: %s", e2)
                    raise HTTPException(status_code=502, detail=f"Upstream error: {e2}")

        models: list[dict[str, Any]] = []
        body = data.get("body", {})
        for group in body.get("inner_models", []):
            for cfg in group.get("model_configs", []):
                model_id = cfg.get("model_id")
                if model_id:
                    models.append(
                        {
                            "id": model_id,
                            "object": "model",
                            "created": int(time.time()),
                            "owned_by": group.get("group_name", "deveco"),
                        }
                    )
        return {"object": "list", "data": models}

    async def _try_refresh_token() -> bool:
        """使用 jwt_token 刷新 access_token，成功则更新内存与配置文件。"""
        jwt_token = config.deveco.auth.jwt_token
        if not jwt_token:
            return False
        base_url = config.deveco.base_url.rstrip("/")
        async with refresh_lock:
            try:
                data = await asyncio.to_thread(refresh_access_token_sync, base_url, jwt_token)
                user_info = data["userInfo"]
                config.deveco.auth.access_token = user_info.get("accessToken", "")
                config.deveco.auth.refresh_token = user_info.get("refreshToken", "")
                config.deveco.auth.user_id = user_info.get("userId", "")
                config.deveco.auth.user_name = user_info.get("name", "")
                save_config(config, config_path)
                logger.info("access_token 刷新成功")
                return True
            except Exception as e:
                logger.error("刷新 access_token 失败: %s", e)
                return False

    async def _token_keeper(interval_s: float) -> None:
        """定时保活刷新（参考 workbuddy2api-panel 的 token keepalive）：
        让 access_token 常新、会话保持活跃；失败时旧 token 仍然有效，下一轮自动重试。"""
        fails = 0
        while True:
            await asyncio.sleep(interval_s)
            if await _try_refresh_token():
                fails = 0
                logger.info("token 保活刷新成功")
            else:
                fails += 1
                if fails >= 3:
                    logger.error(
                        "token 保活连续 %d 次失败，如持续失败请重新登录: python main.py --login", fails
                    )
                else:
                    logger.warning("token 保活刷新失败（连续 %d 次）", fails)

    @app.on_event("startup")
    async def _start_token_keeper() -> None:
        hours = float(config.deveco.keepalive_hours or 0)
        if hours > 0:
            asyncio.create_task(_token_keeper(hours * 3600))
            logger.info("token 保活已启用：每 %s 小时自动刷新一次", hours)
        else:
            logger.info("token 保活未启用（deveco.keepalive_hours = 0）")

    async def _call_upstream(stream: bool, url: str, headers: dict[str, str], body: dict[str, Any], session_id_value: str):
        # 思维链剥离：流式按配置的模型清单缓冲剥离；非流式自动检测（见 _strip_thinking_nonstream）
        strip_thinking = str(body.get("model", "")) in (config.deveco.thinking_models or [])
        # 上游报错姿势不一（HTTP 4xx/5xx + error 对象、200 + error 帧、200 + error 对象，如新会话限流）：
        # 统一摘出 error 并转成规范错误响应；401 保留刷新 token 后重试的路径
        upstream = await client.post(url, headers=headers, json=body)
        if upstream.status_code != 200:
            if upstream.status_code == 401:
                upstream.raise_for_status()
            err = _extract_http_error(upstream)
            status = _upstream_error_status(err)
            logger.warning("上游 HTTP %s: %s", status, err)
            return JSONResponse(status_code=status, content={"error": err})
        if stream:
            upstream_error = _extract_sse_error(upstream.text)
            if upstream_error is not None:
                status = _upstream_error_status(upstream_error)
                logger.warning("上游流式错误 HTTP %s: %s", status, upstream_error)
                return JSONResponse(status_code=status, content={"error": upstream_error})
            return StreamingResponse(
                _stream_response(upstream, session_id_value, strip_thinking),
                media_type="text/event-stream",
            )
        data = upstream.json()
        if isinstance(data, dict) and "error" in data and "choices" not in data:
            err = data["error"] if isinstance(data.get("error"), dict) else {"message": str(data.get("error"))}
            status = _upstream_error_status(err)
            logger.warning("上游错误 HTTP %s: %s", status, err)
            return JSONResponse(status_code=status, content={"error": err})
        return JSONResponse(content=_strip_thinking_nonstream(data))

    @app.post("/v1/chat/completions")
    async def chat_completions(
        request: Request,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    ):
        _verify_api_key(config, credentials)
        try:
            request_body = await request.json()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

        stream = request_body.get("stream", False)
        session_id_value = request_body.get("session_id") or session_id()
        user_msg_id = message_id()

        deveco_body = _build_deveco_body(config, request_body)
        base_url = config.deveco.base_url.rstrip("/")
        path = "/sse/codeGenie/maas/v2"
        if stream:
            url = f"{base_url}{path}/chat/completions"
        else:
            url = f"{base_url}{path}/no-stream/chat/completions"

        logger.info("POST %s model=%s stream=%s", url, deveco_body.get("model"), stream)

        deveco_headers = _build_deveco_headers(config, session_id_value, user_msg_id)
        logger.debug(
            "headers=%s",
            {
                k: (v[:20] + "..." if k.lower() in ("authorization", "jwttoken") else v)
                for k, v in deveco_headers.items()
            },
        )

        try:
            return await _call_upstream(stream, url, deveco_headers, deveco_body, session_id_value)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401 and await _try_refresh_token():
                deveco_headers = _build_deveco_headers(config, session_id_value, user_msg_id)
                try:
                    return await _call_upstream(stream, url, deveco_headers, deveco_body, session_id_value)
                except httpx.HTTPStatusError as e2:
                    text2 = e2.response.text
                    logger.error("刷新后上游请求失败 HTTP %s: %s", e2.response.status_code, text2[:500])
                    raise HTTPException(status_code=502, detail=f"Upstream HTTP error: {text2[:500]}")
            text = ""
            try:
                text = e.response.text
            except Exception:
                pass
            logger.error("上游请求失败 HTTP %s: %s", getattr(e.response, "status_code", "?"), text[:500])
            raise HTTPException(status_code=502, detail=f"Upstream HTTP error: {text[:500]}")
        except httpx.HTTPError as e:
            logger.error("上游请求异常: %s", e)
            raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


async def _stream_response(
    upstream: httpx.Response, session_id_value: str, strip_thinking: bool = False
) -> AsyncGenerator[str, None]:
    """转发上游 SSE。

    strip_thinking=True 时（思考模型），首个 </think> 之前的内容是上游未剥离的
    思维链：先缓冲，边界出现后单帧下发 reasoning_content；流结束仍无边界
    （模型未思考/被截断）则整段按正文兜底，宁可不剥离也不丢内容。
    """
    holding = strip_thinking
    buf = ""
    template: dict[str, Any] = {}
    try:
        async for line in upstream.aiter_lines():
            if not line:
                continue
            if not line.startswith("data: "):
                continue
            payload = line[6:].strip()
            if not payload:  # 上游偶发空 data: 帧（限流场景见过），直接跳过
                continue
            if payload == "[DONE]":
                if holding and buf:
                    yield _sse_chunk(template, {"content": buf})
                yield "data: [DONE]\n\n"
                continue
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                yield f"data: {payload}\n\n"
                continue
            template = chunk

            # 标准化为 OpenAI 格式
            if "choices" in chunk:
                for choice in chunk.get("choices", []):
                    delta = choice.get("delta", {})
                    if "role" not in delta:
                        delta.setdefault("role", "assistant")

            if holding and chunk.get("choices"):
                # ponytail: 上游恒为单 choice，思维链剥离只处理第一个 choice
                choice = chunk["choices"][0]
                delta = choice.get("delta") or {}
                content = delta.get("content") or ""
                if content:
                    buf += content
                    if THINK_END in buf:
                        think, _, rest = buf.partition(THINK_END)
                        holding, buf = False, ""
                        if think.strip():
                            yield _sse_chunk(template, {"role": "assistant", "reasoning_content": think})
                        delta["content"] = rest.lstrip()
                    else:
                        delta["content"] = ""  # 思维链阶段：内容暂存缓冲，仅透传结构帧保活
                if holding and buf and choice.get("finish_reason"):
                    yield _sse_chunk(template, {"content": buf})
                    holding, buf = False, ""
            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
    finally:
        await upstream.aclose()


def create_app(config: Config, config_path: str = "config.toml") -> FastAPI:
    return _create_app(config, config_path)
