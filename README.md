# DevEco2API

把华为 **DevEco Code** 的云端对话能力封装成 **OpenAI 兼容 API**，供任意支持 OpenAI 格式的客户端使用。

## 快速开始

1. 安装依赖：

```bash
uv sync
```

2. 启动服务：
```bash
uv run main.py
```

## 配置

编辑 `config.toml`：

```toml
[server]
host = "127.0.0.1"
port = 10102
api_key = "sk-deveco2api"

[deveco]
callback_port = 10101
model = "GLM-5.1"
client = "cli"
project = "global"
keepalive_hours = 6
thinking_models = ["GLM-5.3"]

[logging]
level = "INFO"
```

| 配置项 | 说明 |
|--------|------|
| `server.port` | 本地 OpenAI 兼容 API 端口 |
| `server.api_key` | 访问本地 API 的密钥 |
| `deveco.callback_port` | 浏览器 OAuth 回调监听端口 |
| `deveco.model` | 默认模型 |
| `deveco.client` / `project` | 请求头 `x-deveco-client` / `x-deveco-project` |
| `deveco.keepalive_hours` | token 保活刷新间隔（小时），0=关闭；默认 6 |
| `deveco.thinking_models` | 流式响应对这些模型剥离思维链到 `reasoning_content`；非流式自动检测 |

## 免费通道（官方口径 + 实测）

官方口径：华为账号登录后即可使用内置免费模型通道，当前为 **GLM-5.1** 与 **GLM-5.3**，
单账号 **50 次请求/分钟**，不设月度总量上限，运行在华为昇腾算力上。

实测（本代理）：真正会拦人的是**新建上游会话**——约 **5 次/分**就 429
`UserSessionLimitExceeded`（冷却约 40–60 秒）；而**同一会话内**连发 64 次（8 并发 / 17s）
全部 200。即官方 50 次/分是**请求**配额，不是新会话配额。

所以 `deveco.session_reuse = true` 时，客户端没带 `session_id` 的请求会复用同一个上游会话
（按 `session_ttl_minutes` 轮换）：实测突发 12 次 12/12 通过、连续 37 次全 200。
默认 `false` 保持"每请求新会话"的老行为（约 5 次/分就撞闸）。

## 使用示例

```bash
# 非流式
curl http://127.0.0.1:10102/v1/chat/completions \
  -H "Authorization: Bearer sk-deveco2api" \
  -H "Content-Type: application/json" \
  -d '{"model":"GLM-5.1","messages":[{"role":"user","content":"你好"}]}'

# 流式
curl -N http://127.0.0.1:10102/v1/chat/completions \
  -H "Authorization: Bearer sk-deveco2api" \
  -H "Content-Type: application/json" \
  -d '{"model":"GLM-5.1","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

## 测试

```bash
uv run test_chat.py
```

## 无头 / 远程服务器登录

服务器上没有浏览器时，有两种方式，token 都会自动写入 config.toml。

### 方式一：SSH 隧道 + 普通登录（最简，推荐）

服务器只打印 OAuth 地址；登录在你的浏览器里直连华为完成（服务器不需要任何
转发界面），回调经由 SSH 隧道送回服务器：

```bash
# 1) 服务器上执行（只打印 URL + 监听回调，不打开浏览器）
uv run main.py --login --no-browser

# 2) 在你的电脑上开隧道（端口以第 1 步 URL 里 port= 为准；默认 10101，
#    被占用时会回退 34567-34570，转发 URL 里实际写的那个端口）
ssh -L 10101:127.0.0.1:10101 <user>@<服务器>

# 3) 在本机浏览器打开第 1 步打印的 URL，完成华为账号登录即可
```

### 方式二：内置中继（不依赖 SSH 转发）

不便于做 SSH 隧道时，用中继模式：本机同时启动「回调等待器 + 登录中继」，
浏览器访问中继地址完成登录（回调由中继转发回服务器）。

```bash
# --tunnel：自动启动 cloudflared 快速隧道并打印外网地址（需已安装 cloudflared）
uv run main.py --login-relay --tunnel

# 无隧道 / 自行端口转发时（浏览器需能访问该地址）
uv run main.py --login-relay --relay-port 8788
```

浏览器打开提示的地址（含口令参数 `?k=...`）完成登录，看到「全部完成」即可关闭页面。
隧道创建失败时会自动退回仅本机模式（日志有提示），可稍后重试或自行做端口转发。
可用参数：`--access-key`（固定口令）、`--timeout`（等待回调秒数，默认 600）。

## 命令行参数

```bash
uv run main.py --port 10102 --no-browser
uv run main.py --login            # 本机登录（浏览器与服务器同机时）
uv run main.py --login-relay      # 无头/远程登录（中继模式，见上）
```
