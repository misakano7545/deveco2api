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

## 无头 / 远程服务器登录（login-relay）

服务器上没有浏览器时（远程 VPS、无 VNC 的无头环境），用中继模式：
本机同时启动「回调等待器 + 登录中继」，浏览器访问中继地址完成华为账号登录
（收尾时发往 localhost 的回调会被中继转发回服务器），token 自动写入 config.toml。

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
