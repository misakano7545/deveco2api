# -*- coding: utf-8 -*-
"""登录中继（无头 / 远程服务器场景）。

用户浏览器（可经隧道）访问本中继，中继把华为登录全流程反代到用户浏览器：
  /  ->  302  ->  /authrouter/forward?...  ->  oauth -> id1 CAS 登录页（全程反代）
用户在真实登录页上完成登录（扫码/短信/密码）。最后一步发往本机 localhost 的
回调会被改写为中继的 /cb/<port>/callback，转发给本机等待器（auth.py 的回调
服务器，默认 10101），由等待器换取并保存 token 到 config.toml；用户在
/finish 页面轮询 /status 看到完成状态。

由 ``main.py --login-relay`` 调用（见 login_via_relay），也可嵌入使用：
先 configure(...) 注入参数，再 create_server()/shutdown_server() 控制生命周期。
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import re
import secrets
import shutil
import subprocess
import threading
import time
import zlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, quote, urlparse

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from .logger import setup_logger

log = setup_logger("deveco2api.login_relay")

# ---------------------------------------------------------------- config (由 configure() 注入)
PORT = 8788
ACCESS_KEY = ""
NONCE = ""
WAITER_PORT = 10101
DEVTOOLS = "http://127.0.0.1:9222"
SESSION_FILE = ""
WAITER_LOG = ""

HOST_ID1 = "id1.cloud.huawei.com"
HOST_OAUTH = "oauth-login.cloud.huawei.com"
HOST_OAUTH1 = "oauth-login1.cloud.huawei.com"
HOST_DEVECO = "cn.devecostudio.huawei.com"
PROXY_HOSTS = [HOST_ID1, HOST_OAUTH, HOST_OAUTH1, HOST_DEVECO]

PREFIX_ROUTES = [
    ("/CAS/", f"https://{HOST_ID1}"),
    ("/DimensionalCode/", f"https://{HOST_ID1}"),
    ("/IDMW/", f"https://{HOST_ID1}"),
    ("/cch5/", f"https://{HOST_ID1}"),
    ("/uniportal/", f"https://{HOST_ID1}"),
    ("/remoteLogin", f"https://{HOST_ID1}"),
    ("/AMW/", f"https://{HOST_ID1}"),
    ("/oauth2/v3/loginCallback", f"https://{HOST_OAUTH1}"),
    ("/oauth2/", f"https://{HOST_OAUTH}"),
    ("/static_rss_vue3/", f"https://{HOST_OAUTH}"),
    ("/authrouter/", f"https://{HOST_DEVECO}"),
    ("/console/", f"https://{HOST_DEVECO}"),
    ("/devspaceapi/", f"https://{HOST_DEVECO}"),
]


def configure(
    *,
    port: int = 8788,
    access_key: str,
    nonce: str,
    waiter_port: int,
    session_file: str,
    waiter_log: str,
    devtools: str = "http://127.0.0.1:9222",
) -> None:
    """注入运行参数（必须在使用前调用）。"""
    global PORT, ACCESS_KEY, NONCE, WAITER_PORT, DEVTOOLS, SESSION_FILE, WAITER_LOG
    PORT = port
    ACCESS_KEY = access_key
    NONCE = nonce
    WAITER_PORT = waiter_port
    DEVTOOLS = devtools
    SESSION_FILE = session_file
    WAITER_LOG = waiter_log


def _apply_url() -> str:
    return (
        f"https://{HOST_DEVECO}/console/DevEcoIDE/apply"
        f"?port={WAITER_PORT}&appid=1008&code={NONCE}"
    )


def _forward_path() -> str:
    return f"/authrouter/forward?redirect_url={quote(_apply_url(), safe='')}"


# ---------------------------------------------------------------- session
class Session:
    def __init__(self):
        self.cookies = []          # {'name','value','domain','path','secure','httpOnly'}
        self.phase = "waiting"     # waiting | completing | done | error
        self.detail = ""
        self.intercepted = ""
        self.transitions = []
        self._task = None

    def to_dict(self):
        return {
            "cookies": self.cookies, "phase": self.phase, "detail": self.detail,
            "intercepted": self.intercepted, "transitions": self.transitions[-60:],
        }

    def save(self):
        if not SESSION_FILE:
            return
        try:
            tmp = SESSION_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.to_dict(), f, ensure_ascii=False)
            os.replace(tmp, SESSION_FILE)
        except Exception as e:
            log.warning("session save failed: %s", e)

    def load(self):
        if not SESSION_FILE:
            return
        try:
            with open(SESSION_FILE, encoding="utf-8") as f:
                d = json.load(f)
            self.cookies = d.get("cookies", [])
            for c in self.cookies:  # 迁移: 无前导点的域级 cookie 归一为 .域
                if c.get("domain") == "huawei.com":
                    c["domain"] = ".huawei.com"
            self.phase = d.get("phase", "waiting")
            self.detail = d.get("detail", "")
            self.intercepted = d.get("intercepted", "")
            self.transitions = d.get("transitions", [])
            if self.phase == "completing":  # relay restarted mid-completion
                self.phase = "error"
                self.detail = "relay 重启过，请点重试。"
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warning("session load failed: %s", e)

    def note(self, s):
        entry = f"{time.strftime('%H:%M:%S')} {s}"
        self.transitions.append(entry)
        self.transitions = self.transitions[-60:]
        log.info("[session] %s", s)
        self.save()

    def reset(self):
        self.cookies = []
        self.phase = "waiting"
        self.detail = ""
        self.intercepted = ""
        self.transitions = []
        self.save()


S = Session()

# ---------------------------------------------------------------- cookie jar
def _domain_match(host, cdomain):
    cd = cdomain.lower()
    if cd.startswith("."):
        d = cd[1:]
        return host == d or host.endswith("." + d)
    return host == cd


def jar_set(c):
    S.cookies = [
        x for x in S.cookies
        if not (x["name"] == c["name"] and x["domain"] == c["domain"] and x["path"] == c["path"])
    ]
    S.cookies.append(c)


def jar_update(resp, req_host):
    for h in resp.headers.get_list("set-cookie"):
        sc = SimpleCookie()
        try:
            sc.load(h)
        except Exception:
            continue
        for name, m in sc.items():
            dom = m["domain"]
            # Cookie 语义: 带 Domain 属性(无论有无前导点) => 后缀匹配; 无属性 => 仅主机匹配
            cdomain = ("." + dom.lstrip(".").lower()) if dom else req_host
            cpath = m["path"] or "/"
            if m["max-age"]:
                try:
                    if int(m["max-age"]) <= 0:
                        S.cookies = [x for x in S.cookies if not (
                            x["name"] == name and _domain_match(req_host, x["domain"]) and x["path"] == cpath)]
                        continue
                except Exception:
                    pass
            if m["expires"]:
                try:
                    if parsedate_to_datetime(m["expires"]) < datetime.now(timezone.utc):
                        S.cookies = [x for x in S.cookies if not (
                            x["name"] == name and _domain_match(req_host, x["domain"]) and x["path"] == cpath)]
                        continue
                except Exception:
                    pass
            jar_set({
                "name": name, "value": m.value, "domain": cdomain, "path": cpath,
                "secure": bool(m["secure"]), "httpOnly": bool(m["httponly"]),
            })
            log.info("[jar+] %s @%s%s", name, cdomain, cpath)


def jar_header(host, path):
    out = []
    for c in S.cookies:
        if _domain_match(host, c["domain"]) and path.startswith(c["path"]):
            out.append(f'{c["name"]}={c["value"]}')
    return "; ".join(out)

# ---------------------------------------------------------------- rewriting
_ATTR_RE = re.compile(
    r'(?P<attr>\b(?:href|src|action|formaction|data-src|data-href|poster)\s*=\s*)'
    r'(?:"(?P<v1>[^"]*)"|\'(?P<v2>[^\']*)\')',
    re.I,
)
_INTEG_RE = re.compile(r'\s+integrity\s*=\s*(?:"[^"]*"|\'[^\']*\')', re.I)
_META_CSP_RE = re.compile(
    r'<meta[^>]*http-equiv\s*=\s*["\']?Content-Security-Policy["\']?[^>]*>', re.I)
_HOST_URL_RE = re.compile(
    r'^(?:https?:)?//(?P<h>' + "|".join(re.escape(h) for h in PROXY_HOSTS) + r')(?::\d{1,5})?(?P<rest>/.*)?$',
    re.I,
)


def rewrite_url(u):
    m = _HOST_URL_RE.match(u.strip())
    if m:
        return m.group("rest") or "/"
    return u


def rewrite_html(text):
    def rep(m):
        v = m.group("v1") if m.group("v1") is not None else m.group("v2")
        nv = rewrite_url(v)
        if nv == v:
            return m.group(0)
        q = '"' if m.group("v1") is not None else "'"
        return f'{m.group("attr")}{q}{nv}{q}'

    text = _ATTR_RE.sub(rep, text)
    text = _INTEG_RE.sub("", text)
    text = _META_CSP_RE.sub("", text)
    return text


# JS/JSON body rewriting: absolute huawei hosts -> relay origin (keeps JS-built
# navigations like location.replace("https://id1...:443/CAS/...") valid & on-relay).
# 另: 登录收尾阶段 consent 页构造的 localhost 回调 -> 反代 /cb/, 由服务器转发给等待器。
_JS_HOSTS = [HOST_ID1, HOST_OAUTH, HOST_OAUTH1, HOST_DEVECO]
_JS_HOST_RE = re.compile(
    r'https?:(?://|\\/\\/|\\\\/\\\\/)(?:'
    + "|".join(re.escape(h) for h in _JS_HOSTS)
    + r')(?::\d{1,5})?',
    re.I,
)


def rewrite_text_body(text, origin):
    # 先替换 localhost 回调(避免第二步注入的 origin 再被本规则二次匹配)
    for p in ("http://localhost:", "http://127.0.0.1:"):
        text = text.replace(p, f"{origin}/cb/")
    text = _JS_HOST_RE.sub(origin, text)
    return text


def rewrite_location(loc):
    if not loc:
        return loc
    return rewrite_url(loc)


_DROP_HEADERS = {
    "content-security-policy", "content-security-policy-report-only",
    "strict-transport-security", "x-frame-options", "expect-ct", "report-to", "nel",
    "set-cookie", "set-cookie2", "content-length", "content-encoding",
    "transfer-encoding", "connection", "keep-alive",
}

# ---------------------------------------------------------------- triggers
def response_trigger_loc(loc):
    # E1 模式: 登录后全链路继续走反代(与用户会话完全同源), 不再拦截给无头浏览器。
    # 保留此函数与 completion 机制用于手动兜底(/debug_complete)。
    return None


def request_trigger(path, query=""):
    # E1 模式: 不拦截, 全部透传。
    return False
    if path.startswith("/oauth2/v3/loginCallback"):
        return True
    if path.startswith("/authrouter/auth"):     # OAuth 返回腿 (auth?redirect=...)
        return True
    if path.startswith("/console/"):            # apply/consent 页(不该由用户浏览器到达)
        return True
    if re.search(r"ticket=[^&]*ST-", query, re.I):  # 登录后的票据跳转(如 /AMW/...?ticket=1ST-...)
        return True
    return False


def _guess_url(path, query):
    # loginCallback 链路在本流程中走 oauth-login1；devecostudio 用于其余返回腿
    base = f"https://{HOST_OAUTH1}" if path.startswith("/oauth2") else f"https://{HOST_DEVECO}"
    return base + path + (("?" + query) if query else "")


def _recover_from_referer(request):
    """从登录页 referer 的 service/loginUrl 参数里恢复出精确的回调 URL。
    登录页 URL 形如 .../loginAuth.html?...&service=<urlencoded 绝对回调 URL>。
    """
    ref = request.headers.get("referer") or ""
    if not ref:
        return None
    try:
        q = parse_qs(urlparse(ref).query, keep_blank_values=True)
    except Exception:
        return None
    for name in ("service", "loginUrl", "casLoginUrl", "casLoginRedirectUrl", "callbackURL"):
        for v in q.get(name, []):
            if v.startswith("http") and "loginCallback" in v:
                return v
    for vals in q.values():  # 兜底: 扫所有参数值
        for v in vals:
            if v.startswith("http") and "/oauth2/v3/loginCallback" in v:
                return v
    return None


def trigger(intercepted, reason):
    if S.phase == "done":
        return
    if intercepted and not S.intercepted:
        S.intercepted = intercepted
    if S.phase != "completing":
        S.phase = "completing"
        S.detail = "服务器正在完成剩余授权步骤…"
        S.note(f"拦截触发({reason}): {S.intercepted[:140]}")
        if S._task is None or S._task.done():
            S._task = asyncio.create_task(completion_flow())

# ---------------------------------------------------------------- completion (headless CDP)
# ponytail: 无头接力兜底——E1 反代模式下不触发，保留用于 /debug_complete 手工排障。
class CDP:
    def __init__(self, ws):
        self.ws = ws
        self._id = 0

    async def call(self, method, params=None, timeout=25):
        self._id += 1
        mid = self._id
        await self.ws.send(json.dumps({"id": mid, "method": method, "params": params or {}}))
        deadline = time.monotonic() + timeout
        while True:
            raw = await asyncio.wait_for(self.ws.recv(), max(0.5, deadline - time.monotonic()))
            msg = json.loads(raw)
            if msg.get("id") == mid:
                if "error" in msg:
                    raise RuntimeError(f"{method} -> {msg['error']}")
                return msg.get("result", {})

    async def eval(self, expr, timeout=25):
        r = await self.call("Runtime.evaluate",
                            {"expression": expr, "returnByValue": True, "awaitPromise": True}, timeout)
        if r.get("exceptionDetails"):
            raise RuntimeError(str(r["exceptionDetails"])[:300])
        return r.get("result", {}).get("value")


async def _pick_target():
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{DEVTOOLS}/json")
        targets = [t for t in r.json() if t.get("type") == "page"]
    if not targets:
        raise RuntimeError("no page target in devtools")
    hw = [t for t in targets if "huawei" in (t.get("url") or "")]
    t = (hw or targets)[0]
    return t


STATE_JS = (
    "JSON.stringify({url: location.href, title: document.title, ready: document.readyState, "
    "text: (document.body && document.body.innerText || '').replace(/\\s+/g,' ').slice(0, 300)})"
)

CLICK_FIND_JS = """
(() => {
  const vis = el => el && el.offsetParent !== null;
  const cb = [...document.querySelectorAll('input[type=checkbox]')].filter(vis).filter(x => !x.checked)[0];
  if (cb) { cb.scrollIntoView({block:'center'}); const r = cb.getBoundingClientRect();
    return JSON.stringify({kind:'checkbox', x: r.x + r.width/2, y: r.y + r.height/2, label:'checkbox'}); }
  const kw = /^(允许|同意|同意并继续|继续|授权|Allow|Agree|Accept|Continue|Authorize)/i;
  const b = [...document.querySelectorAll('button,a,[role=button],.btn,.button,div,span')].filter(vis)
    .filter(x => kw.test((x.innerText||'').trim()) && (x.innerText||'').trim().length <= 24)
    .sort((a,c) => a.getBoundingClientRect().width*a.getBoundingClientRect().height
                 - c.getBoundingClientRect().width*c.getBoundingClientRect().height)[0];
  if (b) { b.scrollIntoView({block:'center'}); const r = b.getBoundingClientRect();
    return JSON.stringify({kind:'button', x: r.x + r.width/2, y: r.y + r.height/2, label:(b.innerText||'').trim().slice(0,20)}); }
  return null;
})()
"""


async def _trusted_click(cdp):
    """Click via real CDP mouse events (untrusted el.click() is often ignored)."""
    r = await cdp.eval(CLICK_FIND_JS)
    if not r:
        return None
    info = json.loads(r)
    x, y = float(info["x"]), float(info["y"])
    await cdp.call("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y})
    await asyncio.sleep(0.15)
    await cdp.call("Input.dispatchMouseEvent",
                   {"type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1})
    await asyncio.sleep(0.08)
    await cdp.call("Input.dispatchMouseEvent",
                   {"type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1})
    return info.get("label")


def _waiter_done():
    if not WAITER_LOG:
        return False
    try:
        size = os.path.getsize(WAITER_LOG)
        with open(WAITER_LOG, "r", encoding="utf-8", errors="replace") as f:
            f.seek(max(0, size - 8000))
            tail = f.read()
        return "LOGIN_OK" in tail
    except Exception:
        return False


async def completion_flow():
    try:
        S.note("completion: 启动无头浏览器接力")
        await asyncio.wait_for(_complete(), 300)
        if S.phase == "completing":
            S.phase = "error"
            S.detail = "完成流程结束但未检测到 LOGIN_OK，请重试。"
            S.save()
    except asyncio.TimeoutError:
        S.phase = "error"
        S.detail = "完成授权超时，请点重试。"
        S.note("completion: 超时")
        S.save()
    except Exception as e:
        S.phase = "error"
        S.detail = f"完成授权出错: {e}"
        S.note(f"completion: 错误 {e!r}")
        S.save()


async def _complete():
    import websockets

    tgt = await _pick_target()
    log.info("[cdp] target: %s", (tgt.get("url") or "")[:110])
    async with websockets.connect(tgt["webSocketDebuggerUrl"], max_size=2 ** 25) as ws:
        cdp = CDP(ws)
        await cdp.call("Network.enable")
        await cdp.call("Network.clearBrowserCookies")
        n_ok = 0
        for c in S.cookies:
            dom = c["domain"].lstrip(".")
            params = {
                "name": c["name"], "value": c["value"], "domain": dom, "path": c["path"],
                "secure": bool(c["secure"]), "httpOnly": bool(c["httpOnly"]),
            }
            try:
                r = await cdp.call("Network.setCookie", params)
                if r.get("success"):
                    n_ok += 1
                else:
                    r2 = await cdp.call("Network.setCookie", {**params, "url": f"https://{dom}/"})
                    if r2.get("success"):
                        n_ok += 1
            except Exception as e:
                log.warning("[cdp] setCookie %s: %s", c["name"], e)
        S.note(f"completion: 注入 {n_ok}/{len(S.cookies)} cookies, 导航 → 拦截点")
        await cdp.call("Page.enable")
        await cdp.call("Page.navigate", {"url": S.intercepted})

        start = time.monotonic()
        last_sig = ""
        last_click = 0.0
        cb_seen = False
        login_page_hits = 0
        while time.monotonic() - start < 260:
            await asyncio.sleep(1.2)
            if _waiter_done():
                S.phase = "done"
                S.detail = "登录与授权已完成，token 已保存。"
                S.note("completion: 检测到 LOGIN_OK ✅ 全部完成")
                S.save()
                return
            try:
                st = json.loads(await cdp.eval(STATE_JS))
            except Exception as e:
                log.warning("[cdp] state eval: %s", e)
                continue
            url = st.get("url", "")
            sig = url[:130] + "|" + st.get("title", "")[:40]
            if sig != last_sig:
                log.info("[cdp] page -> %s | %s", url[:130], st.get("title", "")[:40])
                S.note(f"completion: 页面 → {url[:110]}")
                last_sig = sig
            if "localhost:10101" in url and not cb_seen:
                cb_seen = True
                S.note("completion: 无头浏览器已到达本地回调端口")
            if "/CAS/portal/login" in url:  # loginAuth.html 或 login.html(弹回登录页)
                login_page_hits += 1
                if login_page_hits >= 3:
                    S.phase = "error"
                    S.detail = "会话未能延续（仍显示登录页）。请重新登录后点重试。"
                    S.note("completion: 早退——需要重新登录")
                    S.save()
                    return
            else:
                login_page_hits = 0
            consentish = (
                "/console/DevEcoIDE/apply" in url or "consent" in url.lower()
                or any(k in (st.get("text") or "") for k in ("允许", "同意", "Allow", "Agree"))
            )
            if consentish and time.monotonic() - last_click > 2.5:
                try:
                    clicked = await _trusted_click(cdp)
                    if clicked:
                        S.note(f"completion: 点击「{clicked}」")
                        last_click = time.monotonic()
                except Exception as e:
                    log.warning("[cdp] click: %s", e)
        raise TimeoutError("headless completion poll exhausted")

# ---------------------------------------------------------------- app
app = FastAPI(title="deveco-login-relay")


@app.middleware("http")
async def guard(request: Request, call_next):
    if request.url.path == "/favicon.ico":
        return Response(status_code=204)
    key = request.query_params.get("k") or request.cookies.get("rk")
    if key != ACCESS_KEY:
        return HTMLResponse("<h3>403 Forbidden</h3>", status_code=403)
    resp = await call_next(request)
    if request.query_params.get("k") == ACCESS_KEY and request.cookies.get("rk") != ACCESS_KEY:
        resp.set_cookie("rk", ACCESS_KEY, httponly=True, samesite="lax", max_age=86400)
    return resp


@app.get("/")
async def entry():
    if S.phase in ("completing", "done"):
        return RedirectResponse("/finish")
    return RedirectResponse(_forward_path())


@app.get("/finish")
async def finish():
    return HTMLResponse(FINISH_HTML, headers={"Cache-Control": "no-store"})


@app.get("/status")
async def status():
    return JSONResponse({
        "phase": S.phase, "detail": S.detail,
        "has_intercept": bool(S.intercepted),
        "cookies": [f"{c['name']}@{c['domain']}" for c in S.cookies],
        "log": S.transitions[-40:],
    })


@app.post("/retry")
async def retry():
    if S.intercepted and S.phase in ("error", "completing"):
        S.phase = "completing"
        S.detail = "重试中…"
        S.note("retry")
        if S._task is None or S._task.done():
            S._task = asyncio.create_task(completion_flow())
    return JSONResponse({"ok": True, "phase": S.phase})


@app.get("/reset")
async def reset():
    if S._task and not S._task.done():
        S._task.cancel()
    S.reset()
    return JSONResponse({"ok": True})


@app.api_route("/cb/{port:int}/callback", methods=["GET", "POST"])
async def cb(port: int, request: Request):
    """consent 页把回调发到这里(由 JS 改写而来), 转发给本机等待器, 由它换 token。"""
    body = await request.body()
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.request(request.method, f"http://127.0.0.1:{port}/callback?{request.url.query}", content=body)
        S.note(f"cb: 回调已转发到等待器 (HTTP {r.status_code})")
        S.phase = "completing"
        S.detail = "回调已送达，服务器正在换取 token…"
        if S._task is None or S._task.done():
            S._task = asyncio.create_task(_watch_waiter())
    except Exception as e:
        S.note(f"cb: 转发失败 {e!r}")
    return RedirectResponse("/finish", status_code=303)


async def _watch_waiter():
    for _ in range(180):
        if _waiter_done():
            S.phase = "done"
            S.detail = "登录与授权已完成，token 已保存。"
            S.note("waiter: 检测到 LOGIN_OK ✅ 全部完成")
            S.save()
            return
        await asyncio.sleep(1)
    S.phase = "error"
    S.detail = "回调已送达等待器，但未检测到完成确认。"
    S.note("waiter: 等待超时")
    S.save()


@app.post("/debug_complete")
async def debug_complete(request: Request):
    """演练端点: 手动喂一个拦截 URL 跑完成流程(测试用)。"""
    u = request.query_params.get("u") or ""
    if not u:
        try:
            body = await request.json()
            u = body.get("u") or ""
        except Exception:
            pass
    if not u:
        return JSONResponse({"ok": False, "err": "no url"}, status_code=400)
    if S._task and not S._task.done():
        S._task.cancel()
    S.intercepted = u
    S.phase = "completing"
    S.detail = "debug completion run"
    S.note(f"debug_complete: {u[:120]}")
    if S._task is None or S._task.done():
        S._task = asyncio.create_task(completion_flow())
    return JSONResponse({"ok": True, "intercepted": u[:80]})


def _route_for(path):
    for pref, base in PREFIX_ROUTES:
        if path.startswith(pref):
            return base
    return None


def _build_upstream_headers(request: Request, host: str, path: str):
    drop = {"host", "cookie", "content-length", "accept-encoding", "referer", "origin",
            "connection", "accept-charset"}
    h = {k: v for k, v in request.headers.items() if k.lower() not in drop}
    h["host"] = host
    h["accept-encoding"] = "gzip, deflate"
    ck = jar_header(host, path)
    if ck:
        h["cookie"] = ck
    ref = request.headers.get("referer")
    if ref:
        p = urlparse(ref)
        rp = p.path + (("?" + p.query) if p.query else "")
        rb = _route_for(rp)
        if rb:
            h["referer"] = rb + rp
    og = request.headers.get("origin")
    if og:
        ob = _route_for(urlparse(og).path or "/")
        if ob:
            h["origin"] = ob
    return h


_client: Optional[httpx.AsyncClient] = None


def get_client():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=httpx.Timeout(35.0), follow_redirects=False)
    return _client


def build_response(up: httpx.Response, origin: str, accept_encoding: str = ""):
    body = up.content
    ce = (up.headers.get("content-encoding") or "").lower()
    if ce == "gzip" and body:
        try:
            body = gzip.decompress(body)
        except Exception:
            pass
    elif ce == "deflate" and body:
        try:
            body = zlib.decompress(body, -zlib.MAX_WBITS)
        except Exception:
            try:
                body = zlib.decompress(body)
            except Exception:
                pass
    ct = up.headers.get("content-type", "")
    if body:
        if "text/html" in ct:
            body = rewrite_html(body.decode("utf-8", "replace")).encode("utf-8")
        elif any(t in ct for t in ("javascript", "json", "text/css")):
            body = rewrite_text_body(body.decode("utf-8", "replace"), origin).encode("utf-8")
    headers = {}
    for k, v in up.headers.items():
        if k.lower() in _DROP_HEADERS:
            continue
        headers[k] = v
    if "location" in (k.lower() for k in headers):
        for k in list(headers):
            if k.lower() == "location":
                headers[k] = rewrite_location(headers[k])
    if "text/html" in ct:
        headers.setdefault("Cache-Control", "no-store")
    # 隧道链路对大响应吞吐低(~150KB/s): 文本类资源在源头压缩, 显著加速加载
    if (body and len(body) > 1024 and "gzip" in accept_encoding.lower()
            and any(t in ct for t in ("text/", "javascript", "json", "xml", "svg"))
            and "content-encoding" not in {k.lower() for k in headers}):
        body = gzip.compress(body, 6)
        headers["content-encoding"] = "gzip"
    return Response(content=body, status_code=up.status_code, headers=headers)


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def proxy_any(full_path: str, request: Request):
    path = "/" + full_path
    query = request.url.query or ""

    if request_trigger(path, query):
        u = None
        if re.search(r"ticket=[^&]*ST-", query, re.I):
            base = _route_for(path)
            if base:
                u = base + path + (("?" + query) if query else "")
        if not u:
            u = _recover_from_referer(request) or _guess_url(path, query)
        trigger(u, "req-side")
        return RedirectResponse("/finish", 302)

    base = _route_for(path)
    if not base:
        log.warning("[miss] %s %s", request.method, path[:120])
        return HTMLResponse(f"relay: no route for {path}", status_code=404)

    host = urlparse(base).netloc
    upstream_url = base + path + (("?" + query) if query else "")
    hdrs = _build_upstream_headers(request, host, path)
    body = await request.body() if request.method in ("POST", "PUT", "PATCH", "DELETE") else None
    if "/ajax/" in path or "getLoginWay" in path:  # debug
        log.info("[dbg] %s %s hdr-iface=%s body=%s", request.method, path[:70],
                 request.headers.get("interfaceVersion"), (body or b"")[:1500])
    try:
        up = await get_client().request(request.method, upstream_url, headers=hdrs, content=body)
        get_client().cookies.clear()
    except Exception as e:
        log.error("[err] %s %s -> %r", request.method, path[:100], e)
        return HTMLResponse(f"relay upstream error: {e}", status_code=502)

    jar_update(up, host)
    S.save()

    if up.status_code in (301, 302, 303, 307, 308):  # debug: raw redirect target
        log.info("[loc] %s %s -> %s", request.method, path[:70], (up.headers.get("location") or "")[:200])

    tloc = response_trigger_loc(up.headers.get("location", ""))
    if tloc:
        trigger(tloc, "resp-3xx")
        return RedirectResponse("/finish", 302)

    origin = (
        (request.headers.get("x-forwarded-proto") or request.url.scheme)
        + "://" + (request.headers.get("x-forwarded-host")
                   or request.headers.get("host") or request.url.netloc)
    )
    resp = build_response(up, origin, request.headers.get("accept-encoding", ""))
    log.info("[px] %s %s -> %s %s (%db)", request.method, path[:90], up.status_code, host, len(up.content))
    return resp


FINISH_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DevEco 登录 · 收尾中</title>
<style>
:root{color-scheme:light dark}
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
 font-family:system-ui,-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;background:#f6f7f9;color:#202124}
@media(prefers-color-scheme:dark){body{background:#16181c;color:#e8eaed}.card{background:#23262b!important;border-color:#3a3f46!important}p{color:#9aa0a6}}
.card{background:#fff;border:1px solid #e3e5e8;border-radius:16px;padding:40px 44px;max-width:440px;width:calc(100% - 48px);
 text-align:center;box-shadow:0 12px 40px rgba(0,0,0,.06)}
h1{font-size:20px;margin:0 0 10px}
p{margin:6px 0;font-size:14px;line-height:1.7;color:#5f6368}
.spin{width:34px;height:34px;margin:22px auto 6px;border-radius:50%;border:3px solid #d7dbe0;border-top-color:#4285f4;animation:r 1s linear infinite}
@keyframes r{to{transform:rotate(360deg)}}
.ok{font-size:40px;margin:10px 0 4px}
button{margin-top:18px;padding:10px 22px;border-radius:10px;border:0;background:#4285f4;color:#fff;font-size:14px;cursor:pointer}
button:hover{filter:brightness(1.06)}
.small{font-size:12px;color:#9aa0a6;margin-top:14px}
</style></head>
<body><div class="card">
<div id="icon" class="spin"></div>
<h1 id="t">正在完成授权…</h1>
<p id="d">登录已提交，服务器正在自动完成剩余授权步骤，请稍候。</p>
<button id="retry" style="display:none" onclick="doRetry()">重试完成</button>
<p class="small">完成动作由服务器执行，不依赖本页面；本页可随时关闭。</p>
</div>
<script>
const icon=document.getElementById('icon'),t=document.getElementById('t'),d=document.getElementById('d'),retry=document.getElementById('retry');
async function poll(){
 try{
  const r=await fetch('/status',{cache:'no-store'});const s=await r.json();
  if(s.phase==='done'){icon.className='ok';icon.textContent='✅';t.textContent='全部完成';d.innerHTML='华为账号登录与授权已完成，<b>token 已保存</b>。<br>可以关闭本页面。';retry.style.display='none';return;}
  if(s.phase==='error'){icon.className='ok';icon.textContent='⚠️';t.textContent='收尾遇到问题';d.textContent=s.detail||'请点重试。';retry.style.display='inline-block';}
 }catch(e){}
 setTimeout(poll,1500);
}
async function doRetry(){retry.style.display='none';await fetch('/retry',{method:'POST'});icon.className='spin';icon.textContent='';t.textContent='正在重试…';d.textContent='服务器正在重新完成授权步骤…';poll();}
poll();
</script></body></html>
"""

# ---------------------------------------------------------------- server 生命周期
_server: Optional[uvicorn.Server] = None


def create_server(host: str = "127.0.0.1") -> uvicorn.Server:
    """创建中继 uvicorn server（不阻塞，调用方自行 run/线程）。"""
    global _server
    _server = uvicorn.Server(uvicorn.Config(app, host=host, port=PORT, log_level="warning", access_log=False))
    # 非主线程运行时无法安装信号处理，显式禁用（线程内 server.run() 需要）
    _server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    return _server


def shutdown_server() -> None:
    if _server is not None:
        _server.should_exit = True


# ---------------------------------------------------------------- 隧道与一体化登录
# quick tunnel 分配的真实地址形如 https://xxx-yyy-zzz.trycloudflare.com
# （日志里还会出现 api.trycloudflare.com 等干扰项，必须排除）
_TUNNEL_URL_RE = re.compile(r"https://(?!api\.)[a-z0-9-]+\.trycloudflare\.com")


def start_cloudflared(timeout: float = 30.0):
    """启动 cloudflared 快速隧道指向本机中继。返回 (proc, url)；不可用/失败为 (None, None)。"""
    if not shutil.which("cloudflared"):
        log.warning("未找到 cloudflared，跳过隧道（仅本机可访问）")
        return None, None
    proc = subprocess.Popen(
        ["cloudflared", "tunnel", "--url", f"http://127.0.0.1:{PORT}", "--no-autoupdate"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    url = None
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            m = _TUNNEL_URL_RE.search(line)
            if m:
                url = m.group(0)
                break
    finally:
        # 持续排空输出，防止管道写满阻塞 cloudflared
        threading.Thread(target=lambda: [None for _ in proc.stdout], daemon=True).start()
    if not url:
        log.warning("cloudflared 未成功创建隧道（%.0fs 内无地址，可能被限流/网络受限）；本次仅本机可访问，可稍后重试", timeout)
    return proc, url


def login_via_relay(
    config: Any,
    config_path: str = "config.toml",
    *,
    relay_port: int = 8788,
    access_key: str = "",
    timeout_ms: int = 600_000,
    tunnel: bool = False,
    state_dir: Any = None,
) -> Any:
    """无头/远程登录：本机同时运行「回调等待器 + 登录中继」。

    浏览器打开打印出的地址（可经隧道）完成华为账号授权；回调经中继转发回
    本机等待器，换取 token 并写入 config_path。返回登录结果。
    """
    import uuid

    from .auth import _finalize_login, _start_callback_server, _wait_for_callback, save_login_result

    state = Path(state_dir) if state_dir else Path(config_path).resolve().parent / ".login-relay"
    state.mkdir(parents=True, exist_ok=True)
    access_key = access_key or secrets.token_hex(8)
    client_secret = uuid.uuid4().hex

    waiter_log = state / "deveco-login.log"
    waiter_log.write_text("", encoding="utf-8")  # 清掉旧的 LOGIN_OK，避免误判完成

    # 1) 回调等待器（复用 auth 的回调服务器）
    server, waiter_port = _start_callback_server(config.deveco.callback_port, client_secret)
    log.info("回调等待器已启动: http://127.0.0.1:%s/callback", waiter_port)

    # 2) 登录中继
    configure(
        port=relay_port, access_key=access_key, nonce=client_secret, waiter_port=waiter_port,
        session_file=str(state / "relay_session.json"), waiter_log=str(waiter_log),
    )
    S.reset()
    srv = create_server()
    threading.Thread(target=srv.run, daemon=True).start()

    # 3) 可选隧道
    tunnel_proc, tunnel_url = (None, None)
    if tunnel:
        tunnel_proc, tunnel_url = start_cloudflared()

    entry_url = f"{tunnel_url or f'http://127.0.0.1:{relay_port}'}/?k={access_key}"

    try:
        # 等中继就绪（最多 10s）
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                with httpx.Client(timeout=2) as c:
                    if c.get(f"http://127.0.0.1:{relay_port}/status", params={"k": access_key}).status_code == 200:
                        break
            except Exception:
                pass
            time.sleep(0.2)

        log.info("=" * 64)
        log.info("请在浏览器打开以下地址，用华为账号完成登录：")
        log.info("    %s", entry_url)
        if not tunnel:
            log.info("（服务器无浏览器时：点对点隧道/端口转发后从本机访问；或使用 --tunnel）")
        log.info("回调等待 %d 秒，完成授权后 token 会自动写入 %s", timeout_ms // 1000, config_path)
        log.info("=" * 64)

        # 4) 等浏览器回调 → 换 token → 保存
        callback = _wait_for_callback(server, timeout_ms)
        result = _finalize_login(config, callback)
        save_login_result(config, result, config_path)
        with open(waiter_log, "a", encoding="utf-8") as f:
            f.write("LOGIN_OK\n")  # 中继 /status 据此翻成完成态

        # 5) 给页面留出展示时间，然后收摊
        deadline = time.time() + 30
        while time.time() < deadline and S.phase != "done":
            time.sleep(0.5)
        time.sleep(10)
        log.info("登录完成 ✅ token 已保存到 %s", config_path)
        return result
    finally:
        try:
            server.shutdown()
        except Exception:
            pass
        shutdown_server()
        if tunnel_proc is not None:
            tunnel_proc.terminate()
