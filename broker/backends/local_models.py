from __future__ import annotations

import json
import re
import socket
import uuid
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlsplit
from urllib.request import Request, urlopen

from broker.backends.openai_responses import iter_sse_events
from broker.config import BrokerConfig


DEFAULT_MLX_MODEL = "model"
LOCAL_BACKEND_LABELS = {
    "llama": "llama.cpp",
    "mlx": "MLX Local",
}
LOCAL_BACKEND_URL_ENVS = {
    "llama": "LLAMA_URL",
    "mlx": "MLX_URL",
}


def format_local_backend_error_message(
    settings: dict[str, Any],
    target_url: str,
    message: str,
    *,
    context: str = "request",
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
) -> str:
    prefix = f'{settings["label"]} {context} to {target_url} failed: {message}'
    if (
        settings.get("id") == "llama"
        and settings.get("api_key")
        and is_loopback_target_url_func(target_url)
        and is_invalid_api_key_message_func(message)
    ):
        return (
            prefix
            + " Clear LLAMA_API_KEY if your local llama.cpp server does not use bearer auth."
        )
    return prefix


def build_local_backend_headers(
    settings: dict[str, Any],
    *,
    content_type: str | None = None,
    accept: str | None = None,
    include_api_key: bool = True,
) -> dict[str, str]:
    headers: dict[str, str] = {}
    if content_type:
        headers["Content-Type"] = content_type
    if accept:
        headers["Accept"] = accept
    if include_api_key and settings["api_key"]:
        headers["Authorization"] = f'Bearer {settings["api_key"]}'
    return headers


def local_backend_capabilities(backend: str) -> dict[str, Any]:
    normalized = str(backend or "").strip().lower()
    supports_reasoning_controls = normalized == "llama"
    return {
        "supports_browser_tools": True,
        "supports_tools": True,
        "supports_reasoning_controls": supports_reasoning_controls,
        "supports_chat_template_kwargs": supports_reasoning_controls,
        "supports_reasoning_budget": supports_reasoning_controls,
    }


def local_backend_settings(
    config: BrokerConfig,
    backend: str,
    *,
    default_llama_model: str,
) -> dict[str, Any]:
    normalized = str(backend or "").strip().lower()
    if normalized == "llama":
        configured_model = str(config.llama_model or "").strip()
        return {
            "id": "llama",
            "label": LOCAL_BACKEND_LABELS["llama"],
            "url": str(config.llama_url or "").strip(),
            "configured_model": configured_model,
            "default_model": default_llama_model,
            "api_key": str(config.llama_api_key or "").strip(),
            "url_env": LOCAL_BACKEND_URL_ENVS["llama"],
            "capabilities": local_backend_capabilities("llama"),
        }
    if normalized == "mlx":
        configured_model = str(config.mlx_model or "").strip()
        return {
            "id": "mlx",
            "label": LOCAL_BACKEND_LABELS["mlx"],
            "url": str(config.mlx_url or "").strip(),
            "configured_model": configured_model,
            "default_model": configured_model or DEFAULT_MLX_MODEL,
            "api_key": str(config.mlx_api_key or "").strip(),
            "url_env": LOCAL_BACKEND_URL_ENVS["mlx"],
            "capabilities": local_backend_capabilities("mlx"),
        }
    raise ValueError(f"Unsupported local backend: {backend}")


def derive_openai_models_url(target_url: str) -> str:
    raw_url = str(target_url or "").strip()
    if not raw_url:
        return ""
    try:
        parsed = urlsplit(raw_url)
    except Exception:
        return ""
    segments = [segment for segment in (parsed.path or "").split("/") if segment]
    if len(segments) >= 2 and segments[-2:] == ["chat", "completions"]:
        segments = segments[:-2] + ["models"]
    elif segments and segments[-1] == "completions":
        segments[-1] = "models"
    elif segments and segments[-1] != "models":
        segments.append("models")
    elif not segments:
        segments = ["v1", "models"]
    models_path = "/" + "/".join(segments)
    return parsed._replace(path=models_path, query="", fragment="").geturl()


def derive_llama_models_url(llama_url: str) -> str:
    return derive_openai_models_url(llama_url)


def fetch_local_backend_advertised_models(
    config: BrokerConfig,
    backend: str,
    *,
    timeout_sec: float = 1.0,
    default_llama_model: str,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
) -> tuple[list[str], str, str]:
    settings = local_backend_settings(
        config,
        backend,
        default_llama_model=default_llama_model,
    )
    models_url = derive_openai_models_url(settings["url"])
    if not models_url:
        return [], "", ""
    include_api_key = True
    while True:
        headers = build_local_backend_headers(
            settings,
            accept="application/json",
            include_api_key=include_api_key,
        )
        request = request_class(models_url, method="GET", headers=headers)
        try:
            with urlopen_func(request, timeout=max(0.05, float(timeout_sec))) as response:
                parsed = json.loads(response.read().decode("utf-8"))
            break
        except HTTPError as error:
            message = extract_http_error_message_func(
                error,
                f'{settings["label"]} model discovery failed with status {error.code}.',
            )
            if should_retry_local_backend_without_auth_func(
                backend,
                models_url,
                settings["api_key"],
                message,
            ) and include_api_key:
                include_api_key = False
                continue
            return [], models_url, format_local_backend_error_message(
                settings,
                models_url,
                message,
                context="model discovery",
                is_loopback_target_url_func=is_loopback_target_url_func,
                is_invalid_api_key_message_func=is_invalid_api_key_message_func,
            )
        except URLError as error:
            return [], models_url, f'{settings["label"]} model discovery to {models_url} failed: {error.reason}'
        except socket.timeout:
            return [], models_url, f'{settings["label"]} model discovery to {models_url} timed out.'
        except TimeoutError:
            return [], models_url, f'{settings["label"]} model discovery to {models_url} timed out.'
        except OSError as error:
            return [], models_url, f'{settings["label"]} model discovery to {models_url} failed: {error}'
        except (json.JSONDecodeError, ValueError):
            return [], models_url, f'{settings["label"]} model discovery to {models_url} returned invalid JSON.'
    data = parsed.get("data") if isinstance(parsed, dict) else None
    if not isinstance(data, list):
        return [], models_url, f'{settings["label"]} model discovery to {models_url} returned an invalid payload.'
    model_ids: list[str] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        model_id = str(item.get("id") or "").strip()
        if model_id and model_id not in model_ids:
            model_ids.append(model_id)
    return model_ids, models_url, ""


def fetch_llama_advertised_models(
    config: BrokerConfig,
    *,
    timeout_sec: float = 1.0,
    default_llama_model: str,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
) -> tuple[list[str], str, str]:
    return fetch_local_backend_advertised_models(
        config,
        "llama",
        timeout_sec=timeout_sec,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
    )


def resolve_local_backend_model(
    config: BrokerConfig,
    backend: str,
    *,
    timeout_sec: float = 1.0,
    default_llama_model: str,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
) -> tuple[str, list[str], str, str]:
    settings = local_backend_settings(
        config,
        backend,
        default_llama_model=default_llama_model,
    )
    configured_model = settings["configured_model"]
    advertised_models, _models_url, probe_error = fetch_local_backend_advertised_models(
        config,
        backend,
        timeout_sec=timeout_sec,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
    )
    if configured_model and configured_model in advertised_models:
        return configured_model, advertised_models, "configured", probe_error
    if len(advertised_models) == 1:
        return advertised_models[0], advertised_models, "auto_detected", probe_error
    if advertised_models and configured_model in {"", settings["default_model"]}:
        return advertised_models[0], advertised_models, "auto_detected", probe_error
    if configured_model:
        return configured_model, advertised_models, "configured", probe_error
    if advertised_models:
        return advertised_models[0], advertised_models, "auto_detected", probe_error
    return settings["default_model"], advertised_models, "fallback_default", probe_error


def resolve_llama_model(
    config: BrokerConfig,
    *,
    timeout_sec: float = 1.0,
    default_llama_model: str,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
) -> tuple[str, list[str], str, str]:
    return resolve_local_backend_model(
        config,
        "llama",
        timeout_sec=timeout_sec,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
    )


def local_backend_health(
    config: BrokerConfig,
    backend: str,
    *,
    timeout_sec: float,
    default_llama_model: str,
    socket_module: Any = socket,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
) -> dict[str, Any]:
    settings = local_backend_settings(
        config,
        backend,
        default_llama_model=default_llama_model,
    )
    configured_model = settings["configured_model"]
    target_url = settings["url"]
    models_url = derive_openai_models_url(target_url)
    payload: dict[str, Any] = {
        "configured": bool(target_url),
        "available": False,
        "status": "disabled",
        "url": target_url,
        "host": "",
        "port": None,
        "model": configured_model or settings["default_model"],
        "configured_model": configured_model,
        "advertised_models": [],
        "model_source": "configured" if configured_model else "fallback_default",
        "models_url": models_url,
        "last_error": "",
        "capabilities": dict(settings["capabilities"]),
    }
    if not target_url:
        payload["last_error"] = f'{settings["url_env"]} is not set.'
        return payload
    try:
        parsed = urlparse(target_url)
    except Exception:
        parsed = None
    if parsed is None or parsed.scheme not in {"http", "https"} or not parsed.hostname:
        payload["status"] = "invalid_url"
        payload["last_error"] = f'{settings["url_env"]} is invalid: {target_url}'
        return payload
    host = str(parsed.hostname or "").strip()
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    payload["host"] = host
    payload["port"] = port
    try:
        with socket_module.create_connection((host, port), timeout=max(0.05, float(timeout_sec))):
            pass
    except OSError as error:
        payload["status"] = "unreachable"
        payload["last_error"] = f'Cannot connect to {settings["label"]} at {target_url} ({error}).'
        return payload
    resolved_model, advertised_models, model_source, model_probe_error = resolve_local_backend_model(
        config,
        backend,
        timeout_sec=max(0.05, float(timeout_sec)),
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
    )
    if model_probe_error and is_invalid_api_key_message_func(model_probe_error):
        payload["status"] = "auth_error"
        payload["model"] = resolved_model
        payload["advertised_models"] = advertised_models
        payload["model_source"] = model_source
        payload["last_error"] = model_probe_error
        return payload
    payload["available"] = True
    payload["status"] = "ready"
    payload["model"] = resolved_model
    payload["advertised_models"] = advertised_models
    payload["model_source"] = model_source
    payload["last_error"] = model_probe_error
    return payload


def llama_backend_health(
    config: BrokerConfig,
    *,
    timeout_sec: float,
    default_llama_model: str,
    socket_module: Any = socket,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
) -> dict[str, Any]:
    return local_backend_health(
        config,
        "llama",
        timeout_sec=timeout_sec,
        default_llama_model=default_llama_model,
        socket_module=socket_module,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )


def mlx_backend_health(
    config: BrokerConfig,
    *,
    timeout_sec: float,
    default_llama_model: str,
    socket_module: Any = socket,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
) -> dict[str, Any]:
    return local_backend_health(
        config,
        "mlx",
        timeout_sec=timeout_sec,
        default_llama_model=default_llama_model,
        socket_module=socket_module,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )


def ensure_local_backend_available(
    config: BrokerConfig,
    backend: str,
    *,
    default_llama_model: str,
    local_backend_health_func: Callable[..., dict[str, Any]],
    socket_module: Any = socket,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
) -> dict[str, Any]:
    health = local_backend_health_func(
        config,
        backend,
        timeout_sec=0.35,
        default_llama_model=default_llama_model,
        socket_module=socket_module,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )
    if bool(health.get("available")):
        return health
    settings = local_backend_settings(
        config,
        backend,
        default_llama_model=default_llama_model,
    )
    raise RuntimeError(
        str(health.get("last_error") or f'Cannot connect to {settings["label"]} at {settings["url"]}.')
    )


def ensure_llama_backend_available(
    config: BrokerConfig,
    *,
    default_llama_model: str,
    local_backend_health_func: Callable[..., dict[str, Any]],
    socket_module: Any = socket,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
) -> dict[str, Any]:
    return ensure_local_backend_available(
        config,
        "llama",
        default_llama_model=default_llama_model,
        local_backend_health_func=local_backend_health_func,
        socket_module=socket_module,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )


def ensure_mlx_backend_available(
    config: BrokerConfig,
    *,
    default_llama_model: str,
    local_backend_health_func: Callable[..., dict[str, Any]],
    socket_module: Any = socket,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
) -> dict[str, Any]:
    return ensure_local_backend_available(
        config,
        "mlx",
        default_llama_model=default_llama_model,
        local_backend_health_func=local_backend_health_func,
        socket_module=socket_module,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )


def _flatten_llama_text_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return "".join(_flatten_llama_text_field(item) for item in value)
    if isinstance(value, dict):
        text_value = value.get("text")
        if isinstance(text_value, str):
            return text_value
        for key in ("content", "value", "reasoning", "reasoning_content"):
            nested = value.get(key)
            if nested is None:
                continue
            flattened = _flatten_llama_text_field(nested)
            if flattened:
                return flattened
        return ""
    return str(value)


def _extract_llama_reasoning_text(payload: Any, *, strip: bool = True) -> str:
    if not isinstance(payload, dict):
        return ""
    reasoning = _flatten_llama_text_field(payload.get("reasoning"))
    reasoning_content = _flatten_llama_text_field(payload.get("reasoning_content"))
    if reasoning and reasoning_content and reasoning != reasoning_content:
        merged = f"{reasoning}\n\n{reasoning_content}"
    else:
        merged = reasoning or reasoning_content
    return merged.strip() if strip else merged


def extract_llama_message_parts(message: Any, *, split_stream_text_func: Callable[[str], tuple[str, str]]) -> tuple[str, str]:
    if isinstance(message, dict):
        content = _flatten_llama_text_field(message.get("content"))
        server_reasoning = _extract_llama_reasoning_text(message)
    else:
        content = str(message or "")
        server_reasoning = ""
    visible, inline_reasoning = split_stream_text_func(content)
    reasoning = server_reasoning or inline_reasoning
    return visible, reasoning


def extract_llama_delta_parts(choice: Any) -> tuple[str, str]:
    if not isinstance(choice, dict):
        return "", ""
    delta_obj = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
    content_delta = _flatten_llama_text_field(delta_obj.get("content"))
    reasoning_delta = _extract_llama_reasoning_text(delta_obj, strip=False)
    return content_delta, reasoning_delta


def call_local_backend_completion(
    config: BrokerConfig,
    backend: str,
    messages: list[dict[str, Any]],
    *,
    default_llama_model: str,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | None = None,
    resolved_model: str | None = None,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    stop: list[str] | None = None,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    timeout_sec: float | None = None,
) -> dict[str, Any]:
    settings = local_backend_settings(
        config,
        backend,
        default_llama_model=default_llama_model,
    )
    target_url = settings["url"] or f'(unset {settings["url_env"]})'
    target_model = (
        str(resolved_model or "").strip()
        or resolve_local_backend_model(
            config,
            backend,
            timeout_sec=1.0,
            default_llama_model=default_llama_model,
            request_class=request_class,
            urlopen_func=urlopen_func,
            extract_http_error_message_func=extract_http_error_message_func,
            should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
            is_loopback_target_url_func=is_loopback_target_url_func,
            is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        )[0]
    )
    payload = {
        "model": target_model,
        "messages": messages,
        "temperature": temperature,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = tool_choice or "auto"
    capabilities = local_backend_capabilities(backend)
    if chat_template_kwargs and capabilities.get("supports_chat_template_kwargs"):
        payload["chat_template_kwargs"] = chat_template_kwargs
    if reasoning_budget is not None and capabilities.get("supports_reasoning_budget"):
        payload["reasoning_budget"] = reasoning_budget
    if stop:
        payload["stop"] = stop
    include_api_key = True
    resolved_timeout_sec = max(1.0, float(timeout_sec or config.local_backend_timeout_sec))
    while True:
        headers = build_local_backend_headers(
            settings,
            content_type="application/json",
            include_api_key=include_api_key,
        )
        request = request_class(
            settings["url"],
            method="POST",
            headers=headers,
            data=json.dumps(payload).encode("utf-8"),
        )
        try:
            with urlopen_func(request, timeout=resolved_timeout_sec) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            message = extract_http_error_message_func(
                error,
                f'{settings["label"]} request failed with status {error.code}.',
            )
            if should_retry_local_backend_without_auth_func(
                backend,
                target_url,
                settings["api_key"],
                message,
            ) and include_api_key:
                include_api_key = False
                continue
            raise RuntimeError(
                format_local_backend_error_message(
                    settings,
                    target_url,
                    message,
                    is_loopback_target_url_func=is_loopback_target_url_func,
                    is_invalid_api_key_message_func=is_invalid_api_key_message_func,
                )
            ) from error
        except URLError as error:
            raise RuntimeError(f'{settings["label"]} request to {target_url} failed: {error.reason}') from error
        except socket.timeout as error:
            raise RuntimeError(f'{settings["label"]} request to {target_url} timed out.') from error


def call_local_backend_completion_stream(
    config: BrokerConfig,
    backend: str,
    messages: list[dict[str, Any]],
    *,
    default_llama_model: str,
    split_stream_text_func: Callable[[str], tuple[str, str]],
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | None = None,
    resolved_model: str | None = None,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    stop: list[str] | None = None,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    on_state_delta: Any = None,
    cancel_check: Any = None,
    timeout_sec: float | None = None,
) -> tuple[str, str]:
    settings = local_backend_settings(
        config,
        backend,
        default_llama_model=default_llama_model,
    )
    target_url = settings["url"] or f'(unset {settings["url_env"]})'
    target_model = (
        str(resolved_model or "").strip()
        or resolve_local_backend_model(
            config,
            backend,
            timeout_sec=1.0,
            default_llama_model=default_llama_model,
            request_class=request_class,
            urlopen_func=urlopen_func,
            extract_http_error_message_func=extract_http_error_message_func,
            should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
            is_loopback_target_url_func=is_loopback_target_url_func,
            is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        )[0]
    )
    payload = {
        "model": target_model,
        "messages": messages,
        "temperature": temperature,
        "stream": True,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = tool_choice or "auto"
    capabilities = local_backend_capabilities(backend)
    if chat_template_kwargs and capabilities.get("supports_chat_template_kwargs"):
        payload["chat_template_kwargs"] = chat_template_kwargs
    if reasoning_budget is not None and capabilities.get("supports_reasoning_budget"):
        payload["reasoning_budget"] = reasoning_budget
    if stop:
        payload["stop"] = stop
    include_api_key = True
    resolved_timeout_sec = max(1.0, float(timeout_sec or config.local_backend_timeout_sec))
    while True:
        headers = build_local_backend_headers(
            settings,
            content_type="application/json",
            include_api_key=include_api_key,
        )
        request = request_class(
            settings["url"],
            method="POST",
            headers=headers,
            data=json.dumps(payload).encode("utf-8"),
        )

        accumulated_content = ""
        accumulated_reasoning = ""
        try:
            with urlopen_func(request, timeout=resolved_timeout_sec) as response:
                content_type = str(response.headers.get("Content-Type", "")).lower()
                if "text/event-stream" not in content_type:
                    parsed = json.loads(response.read().decode("utf-8"))
                    choices = parsed.get("choices") if isinstance(parsed.get("choices"), list) else []
                    if choices and isinstance(choices[0], dict):
                        message = choices[0].get("message") if isinstance(choices[0].get("message"), dict) else {}
                        return extract_llama_message_parts(
                            message,
                            split_stream_text_func=split_stream_text_func,
                        )
                    return "", ""
                for event in iter_sse_events(response):
                    if cancel_check and cancel_check():
                        try:
                            response.close()
                        except Exception:
                            pass
                        raise route_request_cancelled_error_cls("Request cancelled by user.")
                    raw_data = str(event.get("data", ""))
                    if not raw_data or raw_data == "[DONE]":
                        continue
                    parsed = json.loads(raw_data)
                    if not isinstance(parsed, dict):
                        continue
                    choices = parsed.get("choices") if isinstance(parsed.get("choices"), list) else []
                    if not choices:
                        continue
                    choice = choices[0] if isinstance(choices[0], dict) else {}
                    content_delta, reasoning_delta = extract_llama_delta_parts(choice)
                    if content_delta:
                        accumulated_content += content_delta
                    if reasoning_delta:
                        accumulated_reasoning += reasoning_delta
                    if content_delta or reasoning_delta:
                        visible, inline_reasoning = split_stream_text_func(accumulated_content)
                        if on_state_delta:
                            on_state_delta(visible, accumulated_reasoning or inline_reasoning)
            visible, inline_reasoning = split_stream_text_func(accumulated_content)
            return visible, accumulated_reasoning or inline_reasoning
        except HTTPError as error:
            message = extract_http_error_message_func(
                error,
                f'{settings["label"]} request failed with status {error.code}.',
            )
            if should_retry_local_backend_without_auth_func(
                backend,
                target_url,
                settings["api_key"],
                message,
            ) and include_api_key:
                include_api_key = False
                continue
            raise RuntimeError(
                format_local_backend_error_message(
                    settings,
                    target_url,
                    message,
                    is_loopback_target_url_func=is_loopback_target_url_func,
                    is_invalid_api_key_message_func=is_invalid_api_key_message_func,
                )
            ) from error
        except URLError as error:
            raise RuntimeError(f'{settings["label"]} request to {target_url} failed: {error.reason}') from error
        except socket.timeout as error:
            raise RuntimeError(f'{settings["label"]} request to {target_url} timed out.') from error


def call_local_backend(
    messages: list[dict[str, str]],
    *,
    config: BrokerConfig,
    backend: str,
    default_llama_model: str,
    llama_chat_system_prompt: str,
    llama_stop_sequences: list[str],
    split_stream_text_func: Callable[[str], tuple[str, str]],
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    cancel_check: Any = None,
) -> tuple[str, str]:
    if cancel_check and cancel_check():
        raise route_request_cancelled_error_cls("Request cancelled by user.")
    health = ensure_local_backend_available(
        config,
        backend,
        default_llama_model=default_llama_model,
        local_backend_health_func=local_backend_health,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )
    resolved_model = str(health.get("model") or "").strip() or resolve_local_backend_model(
        config,
        backend,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
    )[0]
    guarded_messages = list(messages)
    stop_sequences: list[str] | None = None
    if backend == "llama":
        guarded_messages = [{"role": "system", "content": llama_chat_system_prompt}, *guarded_messages]
        stop_sequences = llama_stop_sequences
    parsed = call_local_backend_completion(
        config,
        backend,
        guarded_messages,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        resolved_model=resolved_model,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        stop=stop_sequences,
    )
    if cancel_check and cancel_check():
        raise route_request_cancelled_error_cls("Request cancelled by user.")
    choices = parsed.get("choices") if isinstance(parsed.get("choices"), list) else []
    if not choices or not isinstance(choices[0], dict):
        return "", ""
    message = choices[0].get("message") if isinstance(choices[0].get("message"), dict) else {}
    return extract_llama_message_parts(
        message,
        split_stream_text_func=split_stream_text_func,
    )


def call_local_backend_stream(
    messages: list[dict[str, str]],
    *,
    config: BrokerConfig,
    backend: str,
    default_llama_model: str,
    llama_chat_system_prompt: str,
    llama_stop_sequences: list[str],
    split_stream_text_func: Callable[[str], tuple[str, str]],
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    cancel_check: Any = None,
    on_state_delta: Any = None,
) -> tuple[str, str]:
    if cancel_check and cancel_check():
        raise route_request_cancelled_error_cls("Request cancelled by user.")
    health = ensure_local_backend_available(
        config,
        backend,
        default_llama_model=default_llama_model,
        local_backend_health_func=local_backend_health,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
    )
    resolved_model = str(health.get("model") or "").strip() or resolve_local_backend_model(
        config,
        backend,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
    )[0]
    guarded_messages = list(messages)
    stop_sequences: list[str] | None = None
    if backend == "llama":
        guarded_messages = [{"role": "system", "content": llama_chat_system_prompt}, *guarded_messages]
        stop_sequences = llama_stop_sequences
    answer_text, reasoning_text = call_local_backend_completion_stream(
        config,
        backend,
        guarded_messages,
        default_llama_model=default_llama_model,
        split_stream_text_func=split_stream_text_func,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        route_request_cancelled_error_cls=route_request_cancelled_error_cls,
        resolved_model=resolved_model,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        stop=stop_sequences,
        cancel_check=cancel_check,
        on_state_delta=on_state_delta,
    )
    if cancel_check and cancel_check():
        raise route_request_cancelled_error_cls("Request cancelled by user.")
    return answer_text, reasoning_text


def call_llama_completion(
    config: BrokerConfig,
    messages: list[dict[str, Any]],
    *,
    default_llama_model: str,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | None = None,
    resolved_model: str | None = None,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    stop: list[str] | None = None,
    temperature: float = 0.1,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    return call_local_backend_completion(
        config,
        "llama",
        messages,
        default_llama_model=default_llama_model,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        tools=tools,
        tool_choice=tool_choice,
        resolved_model=resolved_model,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        stop=stop,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def call_llama_completion_stream(
    config: BrokerConfig,
    messages: list[dict[str, Any]],
    *,
    default_llama_model: str,
    split_stream_text_func: Callable[[str], tuple[str, str]],
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    tools: list[dict[str, Any]] | None = None,
    tool_choice: str | None = None,
    resolved_model: str | None = None,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    stop: list[str] | None = None,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    on_state_delta: Any = None,
    cancel_check: Any = None,
) -> tuple[str, str]:
    return call_local_backend_completion_stream(
        config,
        "llama",
        messages,
        default_llama_model=default_llama_model,
        split_stream_text_func=split_stream_text_func,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        route_request_cancelled_error_cls=route_request_cancelled_error_cls,
        tools=tools,
        tool_choice=tool_choice,
        resolved_model=resolved_model,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        stop=stop,
        temperature=temperature,
        max_tokens=max_tokens,
        on_state_delta=on_state_delta,
        cancel_check=cancel_check,
    )


def call_llama(
    messages: list[dict[str, str]],
    *,
    config: BrokerConfig,
    default_llama_model: str,
    llama_chat_system_prompt: str,
    llama_stop_sequences: list[str],
    split_stream_text_func: Callable[[str], tuple[str, str]],
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    cancel_check: Any = None,
) -> tuple[str, str]:
    return call_local_backend(
        messages,
        config=config,
        backend="llama",
        default_llama_model=default_llama_model,
        llama_chat_system_prompt=llama_chat_system_prompt,
        llama_stop_sequences=llama_stop_sequences,
        split_stream_text_func=split_stream_text_func,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        route_request_cancelled_error_cls=route_request_cancelled_error_cls,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        cancel_check=cancel_check,
    )


def call_llama_stream(
    messages: list[dict[str, str]],
    *,
    config: BrokerConfig,
    default_llama_model: str,
    llama_chat_system_prompt: str,
    llama_stop_sequences: list[str],
    split_stream_text_func: Callable[[str], tuple[str, str]],
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    extract_http_error_message_func: Callable[[HTTPError, str], str],
    should_retry_local_backend_without_auth_func: Callable[[str, str, str, str], bool],
    is_loopback_target_url_func: Callable[[str], bool],
    is_invalid_api_key_message_func: Callable[[str], bool],
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    cancel_check: Any = None,
    on_state_delta: Any = None,
) -> tuple[str, str]:
    return call_local_backend_stream(
        messages,
        config=config,
        backend="llama",
        default_llama_model=default_llama_model,
        llama_chat_system_prompt=llama_chat_system_prompt,
        llama_stop_sequences=llama_stop_sequences,
        split_stream_text_func=split_stream_text_func,
        request_class=request_class,
        urlopen_func=urlopen_func,
        extract_http_error_message_func=extract_http_error_message_func,
        should_retry_local_backend_without_auth_func=should_retry_local_backend_without_auth_func,
        is_loopback_target_url_func=is_loopback_target_url_func,
        is_invalid_api_key_message_func=is_invalid_api_key_message_func,
        route_request_cancelled_error_cls=route_request_cancelled_error_cls,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        cancel_check=cancel_check,
        on_state_delta=on_state_delta,
    )


def parse_tool_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str):
        parsed = json.loads(arguments)
        if not isinstance(parsed, dict):
            raise ValueError("Tool arguments JSON must decode to an object.")
        return parsed
    raise ValueError("Unsupported tool arguments shape from llama.cpp.")


def normalize_mlx_tool_name(
    tool_name: str,
    *,
    model_browser_tool_names: set[str] | None = None,
    legacy_model_browser_tool_names: set[str] | None = None,
) -> str:
    normalized = str(tool_name or "").strip()
    if not normalized:
        return ""
    if normalized.startswith("functions."):
        normalized = normalized.split(".", 1)[1]
    elif normalized.startswith("function."):
        normalized = normalized.split(".", 1)[1]
    if normalized.startswith("browser_"):
        normalized = "browser." + normalized[len("browser_"):]
    elif "." not in normalized:
        normalized = f"browser.{normalized}"
    allowed = set(model_browser_tool_names or set()) | set(legacy_model_browser_tool_names or set())
    if not allowed or normalized in allowed:
        return normalized
    return normalized


def run_local_backend_browser_agent(
    session_id: str,
    messages: list[dict[str, Any]],
    allowed_hosts: list[str],
    max_steps: int,
    *,
    config: BrokerConfig,
    backend: str,
    ensure_local_backend_available_func: Callable[..., dict[str, Any]],
    local_backend_settings_func: Callable[..., dict[str, Any]],
    call_local_backend_completion_func: Callable[..., dict[str, Any]],
    browser_automation: Any,
    extension_relay: Any,
    llama_browser_tools: list[dict[str, Any]],
    translate_model_browser_tool_func: Callable[[str, dict[str, Any]], dict[str, Any]],
    llama_browser_agent_system_prompt: str,
    llama_force_browser_action_instructions: str,
    unlimited_browser_agent_steps: int,
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    cancel_check: Any = None,
) -> str:
    if cancel_check and cancel_check():
        raise route_request_cancelled_error_cls("Request cancelled by user.")
    backend_health = ensure_local_backend_available_func(config, backend)
    default_model = local_backend_settings_func(config, backend)["default_model"]
    resolved_model = str(backend_health.get("model") or "").strip() or default_model
    session = browser_automation.session_create(
        {
            "policy": {
                "domainAllowlist": allowed_hosts,
                "approvalMode": "auto-approve",
            }
        }
    )
    run = browser_automation.run_start(
        {
            "sessionId": session["sessionId"],
            "capabilityToken": session["capabilityToken"],
        }
    )
    agent_messages: list[dict[str, Any]] = [
        {
            "role": "system",
            "content": f"{llama_browser_agent_system_prompt} {llama_force_browser_action_instructions}",
        },
        *messages,
    ]

    remaining_steps = int(max_steps)
    if remaining_steps < 0:
        raise ValueError("max_steps must be a non-negative integer.")
    infinite_mode = remaining_steps == unlimited_browser_agent_steps
    try:
        used_browser_tools = False
        while infinite_mode or remaining_steps > 0:
            if cancel_check and cancel_check():
                raise route_request_cancelled_error_cls("Request cancelled by user.")

            response = call_local_backend_completion_func(
                backend,
                agent_messages,
                tools=llama_browser_tools,
                tool_choice="required" if not used_browser_tools else "auto",
                resolved_model=resolved_model,
                chat_template_kwargs=chat_template_kwargs,
                reasoning_budget=reasoning_budget,
                temperature=0.1,
                timeout_sec=config.local_backend_browser_timeout_sec,
            )
            if cancel_check and cancel_check():
                raise route_request_cancelled_error_cls("Request cancelled by user.")
            message = response["choices"][0].get("message", {})
            content = str(message.get("content", "") or "")
            tool_calls = message.get("tool_calls") or []

            if not tool_calls:
                return content

            agent_messages.append(
                {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": tool_calls,
                }
            )

            for tool_call in tool_calls:
                if cancel_check and cancel_check():
                    raise route_request_cancelled_error_cls("Request cancelled by user.")
                tool_call_id = str(tool_call.get("id") or f"toolcall_{uuid.uuid4().hex[:8]}")
                function = tool_call.get("function") or {}
                tool_name = str(function.get("name", "")).strip()

                try:
                    tool_args = parse_tool_arguments(function.get("arguments", {}))
                    translated = translate_model_browser_tool_func(tool_name, tool_args)
                    envelope = browser_automation.execute_tool(
                        tool_name=str(translated["tool_name"]),
                        args={
                            "sessionId": session["sessionId"],
                            "runId": run["runId"],
                            "toolCallId": tool_call_id,
                            "capabilityToken": session["capabilityToken"],
                            "args": dict(translated["args"]),
                        },
                        relay=extension_relay,
                        timeout_sec=config.browser_command_timeout_sec,
                    )
                    tool_payload = {
                        "success": envelope.get("success"),
                        "data": envelope.get("data"),
                        "error": envelope.get("error"),
                        "policy": envelope.get("policy"),
                    }
                except Exception as error:
                    tool_payload = {
                        "success": False,
                        "data": None,
                        "error": {
                            "code": "tool_execution_error",
                            "message": str(error),
                        },
                        "policy": None,
                    }

                agent_messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": json.dumps(tool_payload),
                    }
                )
            used_browser_tools = True
            if not infinite_mode:
                remaining_steps -= 1
    finally:
        browser_automation.close_session(session["sessionId"], run["runId"])

    return "I could not complete the browser task within the allowed number of steps."


def run_llama_browser_agent(
    session_id: str,
    messages: list[dict[str, Any]],
    allowed_hosts: list[str],
    max_steps: int,
    *,
    config: BrokerConfig,
    ensure_local_backend_available_func: Callable[..., dict[str, Any]],
    local_backend_settings_func: Callable[..., dict[str, Any]],
    call_local_backend_completion_func: Callable[..., dict[str, Any]],
    browser_automation: Any,
    extension_relay: Any,
    llama_browser_tools: list[dict[str, Any]],
    translate_model_browser_tool_func: Callable[[str, dict[str, Any]], dict[str, Any]],
    llama_browser_agent_system_prompt: str,
    llama_force_browser_action_instructions: str,
    unlimited_browser_agent_steps: int,
    route_request_cancelled_error_cls: type[BaseException] = RuntimeError,
    chat_template_kwargs: dict[str, Any] | None = None,
    reasoning_budget: int | None = None,
    cancel_check: Any = None,
) -> str:
    return run_local_backend_browser_agent(
        session_id,
        messages,
        allowed_hosts,
        max_steps,
        config=config,
        backend="llama",
        ensure_local_backend_available_func=ensure_local_backend_available_func,
        local_backend_settings_func=local_backend_settings_func,
        call_local_backend_completion_func=call_local_backend_completion_func,
        browser_automation=browser_automation,
        extension_relay=extension_relay,
        llama_browser_tools=llama_browser_tools,
        translate_model_browser_tool_func=translate_model_browser_tool_func,
        llama_browser_agent_system_prompt=llama_browser_agent_system_prompt,
        llama_force_browser_action_instructions=llama_force_browser_action_instructions,
        unlimited_browser_agent_steps=unlimited_browser_agent_steps,
        route_request_cancelled_error_cls=route_request_cancelled_error_cls,
        chat_template_kwargs=chat_template_kwargs,
        reasoning_budget=reasoning_budget,
        cancel_check=cancel_check,
    )


def _extract_json_payload(value: str) -> Any | None:
    text = str(value or '').strip()
    if not text:
        return None

    candidates: list[str] = [
        *[
            str(match.group(1) or '').strip()
            for match in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.I)
            if str(match.group(1) or '').strip()
        ],
        text,
    ]
    seen: set[str] = set()
    decoder = json.JSONDecoder()

    def _decode_payloads(payload: str) -> list[Any]:
        decoded = []
        for index, char in enumerate(payload):
            if char not in '[{':
                continue
            try:
                parsed, _ = decoder.raw_decode(payload, idx=index)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, (dict, list)):
                decoded.append(parsed)
        return decoded

    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        decoded = _decode_payloads(candidate)
        if decoded:
            return decoded[0]
    return None


def _extract_json_payloads(value: str) -> list[Any]:
    text = str(value or '').strip()
    if not text:
        return []

    candidates: list[str] = [
        *[
            str(match.group(1) or '').strip()
            for match in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", text, re.I)
            if str(match.group(1) or '').strip()
        ],
        text,
    ]
    seen: set[str] = set()
    decoder = json.JSONDecoder()
    extracted: list[Any] = []

    def _decode_payloads(payload: str) -> list[Any]:
        decoded = []
        for index, char in enumerate(payload):
            if char not in '[{':
                continue
            try:
                parsed, _ = decoder.raw_decode(payload, idx=index)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, (dict, list)):
                decoded.append(parsed)
        return decoded

    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        extracted.extend(_decode_payloads(candidate))

    return extracted


def _coerce_mlx_tool_call(
    raw_call: dict[str, Any],
    *,
    normalize_mlx_tool_name_func: Callable[[str], str],
    parse_tool_arguments_func: Callable[[Any], dict[str, Any]],
) -> dict[str, Any] | None:
    if not isinstance(raw_call, dict):
        return None

    if isinstance(raw_call.get('function'), dict):
        function = raw_call.get('function') or {}
        if isinstance(function, dict) and 'name' in function:
            raw_call = dict(raw_call)
            raw_call['name'] = function.get('name', raw_call.get('name'))
            raw_call['arguments'] = function.get('arguments', raw_call.get('arguments'))

    tool_name = str(raw_call.get('name') or raw_call.get('tool') or raw_call.get('tool_name') or '').strip()
    tool_name = normalize_mlx_tool_name_func(tool_name)
    if not tool_name:
        return None

    arguments = raw_call.get('arguments')
    if arguments is None:
        arguments = raw_call.get('args')
    if arguments is None:
        arguments = raw_call.get('parameters')

    try:
        parsed_args = parse_tool_arguments_func(arguments)
    except Exception:
        return None

    tool_call_id = str(
        raw_call.get('tool_call_id')
        or raw_call.get('id')
        or raw_call.get('call_id')
        or f"tool_{uuid.uuid4().hex[:8]}"
    ).strip()
    if not tool_call_id:
        tool_call_id = f"tool_{uuid.uuid4().hex[:8]}"

    return {
        'name': tool_name,
        'arguments': parsed_args,
        'tool_call_id': tool_call_id,
    }


def _extract_mlx_tool_calls(
    value: str,
    *,
    extract_json_payloads_func: Callable[[str], list[Any]],
    coerce_mlx_tool_call_func: Callable[[dict[str, Any]], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    parsed_payloads = extract_json_payloads_func(value)
    if not parsed_payloads:
        return []

    calls: list[dict[str, Any]] = []
    for parsed in parsed_payloads:
        if not isinstance(parsed, (dict, list)):
            continue
        tool_call_payloads = [parsed] if isinstance(parsed, dict) else parsed
        for raw in tool_call_payloads:
            if not isinstance(raw, dict):
                continue
            if 'tool_calls' in raw and isinstance(raw.get('tool_calls'), list):
                for nested in raw.get('tool_calls', []):
                    coerced = coerce_mlx_tool_call_func(nested)
                    if coerced is not None:
                        calls.append(coerced)
                continue
            coerced = coerce_mlx_tool_call_func(raw)
            if coerced is not None:
                calls.append(coerced)
    return calls


def build_models_payload(
    *,
    codex_status: str,
    llama_health: dict[str, Any],
    mlx_health: dict[str, Any],
    local_backend_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels = dict(local_backend_labels or LOCAL_BACKEND_LABELS)
    codex_capabilities = {
        'supports_browser_tools': True,
        'supports_tools': True,
        'supports_reasoning_controls': False,
        'supports_chat_template_kwargs': False,
        'supports_reasoning_budget': False,
    }
    return {
        'backends': [
            {
                'id': 'codex',
                'label': 'Codex',
                'available': codex_status != 'disabled',
                'status': codex_status,
                'capabilities': codex_capabilities,
            },
            {
                'id': 'llama',
                'label': labels['llama'],
                'available': bool(llama_health.get('available')),
                'status': str(llama_health.get('status') or 'disabled'),
                'capabilities': dict(llama_health.get('capabilities') or {}),
            },
            {
                'id': 'mlx',
                'label': labels['mlx'],
                'available': bool(mlx_health.get('available')),
                'status': str(mlx_health.get('status') or 'disabled'),
                'capabilities': dict(mlx_health.get('capabilities') or {}),
            },
        ],
        'llama': llama_health,
        'mlx': mlx_health,
    }
