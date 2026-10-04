# -*- coding: utf-8 -*-
"""config.toml 加载与持久化。"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import tomli_w


@dataclass
class DevEcoAuthConfig:
    jwt_token: str = ""
    access_token: str = ""
    refresh_token: str = ""
    user_id: str = ""
    user_name: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DevEcoAuthConfig":
        return cls(
            jwt_token=data.get("jwt_token", ""),
            access_token=data.get("access_token", ""),
            refresh_token=data.get("refresh_token", ""),
            user_id=data.get("user_id", ""),
            user_name=data.get("user_name", ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "jwt_token": self.jwt_token,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "user_id": self.user_id,
            "user_name": self.user_name,
        }


@dataclass
class DevEcoConfig:
    base_url: str = "https://cn.devecostudio.huawei.com"
    auth_url: str = "console/DevEcoIDE/apply"
    temp_token_check_url: str = "authrouter/auth/api/temptoken/check"
    jwt_token_check_url: str = "authrouter/auth/api/jwToken/check"
    app_id: str = "1008"
    callback_port: int = 10101
    model: str = "GLM-5.1"
    client: str = "cli"
    project: str = "global"
    user_agent: str = "deveco/0.2.0"
    keepalive_hours: float = 6.0  # token 保活刷新间隔（小时），0=关闭
    thinking_models: list[str] = field(default_factory=lambda: ["GLM-5.3"])  # 流式剥离思维链的模型
    # session_reuse: 客户端未带 session_id 时复用同一个上游会话。
    # 实测：上游真正会拦的是「新建会话」——约 5 次/分就 429 (UserSessionLimitExceeded)，
    # 而同一会话内连发 64 次（8 并发/17s）全 200；官方口径的 50 次/分是请求配额。
    # True 把实际可用速率从 ~5 次/分提到上游请求配额量级；False 保持每请求新会话。
    session_reuse: bool = False
    session_ttl_minutes: int = 30  # 复用会话的轮换间隔（分钟）
    auth: DevEcoAuthConfig = field(default_factory=DevEcoAuthConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DevEcoConfig":
        return cls(
            base_url=data.get("base_url", "https://cn.devecostudio.huawei.com"),
            auth_url=data.get("auth_url", "console/DevEcoIDE/apply"),
            temp_token_check_url=data.get(
                "temp_token_check_url", "authrouter/auth/api/temptoken/check"
            ),
            jwt_token_check_url=data.get(
                "jwt_token_check_url", "authrouter/auth/api/jwToken/check"
            ),
            app_id=str(data.get("app_id", "1008")),
            callback_port=int(data.get("callback_port", 10101)),
            model=data.get("model", "GLM-5.1"),
            client=data.get("client", "cli"),
            project=data.get("project", "global"),
            user_agent=data.get(
                "user_agent",
                "deveco/0.2.0",
            ),
            keepalive_hours=float(data.get("keepalive_hours", 6.0)),
            thinking_models=list(data.get("thinking_models", ["GLM-5.3"])),
            session_reuse=bool(data.get("session_reuse", False)),
            session_ttl_minutes=int(data.get("session_ttl_minutes", 30)),
            auth=DevEcoAuthConfig.from_dict(data.get("auth", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "auth_url": self.auth_url,
            "temp_token_check_url": self.temp_token_check_url,
            "jwt_token_check_url": self.jwt_token_check_url,
            "app_id": self.app_id,
            "callback_port": self.callback_port,
            "model": self.model,
            "client": self.client,
            "project": self.project,
            "user_agent": self.user_agent,
            "keepalive_hours": self.keepalive_hours,
            "thinking_models": self.thinking_models,
            "session_reuse": self.session_reuse,
            "session_ttl_minutes": self.session_ttl_minutes,
            "auth": self.auth.to_dict(),
        }


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 10102
    api_key: str = "sk-deveco2api"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ServerConfig":
        return cls(
            host=data.get("host", "127.0.0.1"),
            port=int(data.get("port", 10102)),
            api_key=data.get("api_key", "sk-deveco2api"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "api_key": self.api_key,
        }


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    deveco: DevEcoConfig = field(default_factory=DevEcoConfig)
    logging: dict[str, Any] = field(default_factory=lambda: {"level": "INFO"})

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        return cls(
            server=ServerConfig.from_dict(data.get("server", {})),
            deveco=DevEcoConfig.from_dict(data.get("deveco", {})),
            logging=data.get("logging", {"level": "INFO"}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "server": self.server.to_dict(),
            "deveco": self.deveco.to_dict(),
            "logging": self.logging,
        }


def load_config(path: str | Path = "config.toml") -> Config:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path.absolute()}")
    with path.open("rb") as f:
        data = tomllib.load(f)
    return Config.from_dict(data)


def save_config(config: Config, path: str | Path = "config.toml") -> None:
    path = Path(path)
    with path.open("wb") as f:
        tomli_w.dump(config.to_dict(), f)
