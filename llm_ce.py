"""
Módulo de análise de relatórios de Ciclos de Estudos (CEs) por LLM.

Função principal: analisar_relatorio_ce
  - Recebe PDF bytes do relatório pedagógico de um CE
  - Envia ao LLM com o system prompt de pareceres
  - Devolve o parecer em HTML
"""

from __future__ import annotations

import base64
import io
import json
import os
import random
import re
import time
from pathlib import Path

import anthropic
from pypdf import PdfReader
from openai import OpenAI

from sigarra import load_env
from logger import AuditoriaLogger

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROMPTS_DIR = _SCRIPT_DIR / "prompts"

_SYSTEM_PROMPT: str | None = None


def _carregar_system_prompt() -> str:
    global _SYSTEM_PROMPT
    if _SYSTEM_PROMPT is None:
        # Procurar em prompts/ primeiro, depois na raiz
        for p in [_PROMPTS_DIR / "system_prompt.txt", _SCRIPT_DIR / "system_prompt.txt"]:
            if p.exists():
                _SYSTEM_PROMPT = p.read_text(encoding="utf-8").strip()
                return _SYSTEM_PROMPT
        raise FileNotFoundError(
            "system_prompt.txt não encontrado em prompts/ nem na raiz do projecto."
        )
    return _SYSTEM_PROMPT


def _is_retryable_llm_error(exc: Exception) -> bool:
    """Heurística para erros transitórios (429/5xx/timeouts/conectividade)."""
    code = (
        getattr(exc, "status_code", None)
        or getattr(exc, "status", None)
        or getattr(exc, "code", None)
    )
    if isinstance(code, int) and (code == 429 or 500 <= code < 600):
        return True
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if isinstance(status_code, int) and (status_code == 429 or 500 <= status_code < 600):
        return True
    msg = str(exc).lower()
    retry_hints = (
        "rate limit", "429", "too many requests", "temporarily unavailable",
        "service unavailable", "overloaded", "timeout", "timed out",
        "connection reset", "connection aborted", "try again",
    )
    return any(h in msg for h in retry_hints)


def _garantir_api_key(provider: str) -> None:
    """Verifica que a API key do provider está configurada."""
    load_env()
    key_map = {"openai": "OPENAI_API_KEY", "iaedu": "IAEDU_API_KEY"}
    env_key = key_map.get(provider, "ANTHROPIC_API_KEY")
    if provider == "iaedu":
        # iaedu usa IAEDU_API_KEY ou IAEDU_ENDPOINT
        key = os.environ.get("IAEDU_API_KEY", "").strip()
        endpoint = os.environ.get("IAEDU_ENDPOINT", "").strip()
        if not key or not endpoint:
            raise RuntimeError(
                "IAEDU_API_KEY e IAEDU_ENDPOINT não configurados. Defina-os em .env."
            )
        return
    key = os.environ.get(env_key, "").strip()
    if not key:
        raise RuntimeError(
            f"{env_key} não configurado. Defina-o em .env antes de iniciar o servidor."
        )


def _pdf_to_text(pdf_bytes: bytes, max_pages: int = 50) -> str:
    """Extrai texto de um PDF usando pypdf."""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        partes = []
        for page in reader.pages[:max_pages]:
            t = (page.extract_text() or "").strip()
            if t:
                partes.append(t)
        return "\n\n".join(partes).strip() or "[sem texto extraível do PDF]"
    except Exception as e:
        return f"[erro ao extrair texto do PDF: {e}]"


def _carregar_precos() -> dict[str, tuple[float, float]]:
    """Carrega tabela de preços de LLM_PRICING_JSON."""
    load_env()
    payload = os.environ.get("LLM_PRICING_JSON", "").strip()
    if not payload:
        return {}
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    precos = {}
    for modelo, val in data.items():
        try:
            if isinstance(val, (list, tuple)) and len(val) == 2:
                precos[str(modelo)] = (float(val[0]), float(val[1]))
            elif isinstance(val, dict):
                precos[str(modelo)] = (float(val.get("input", 0)), float(val.get("output", 0)))
        except (TypeError, ValueError):
            continue
    return dict(sorted(precos.items(), key=lambda x: len(x[0]), reverse=True))


_PRECOS: dict | None = None


def _estimar_custo(modelo: str, input_tokens: int, output_tokens: int) -> float | None:
    global _PRECOS
    if _PRECOS is None:
        _PRECOS = _carregar_precos()
    precos = _PRECOS.get(modelo)
    if precos is None:
        for chave, custos in _PRECOS.items():
            if modelo.startswith(chave):
                precos = custos
                break
    if precos is None:
        return None
    return (input_tokens * precos[0] + output_tokens * precos[1]) / 1_000_000


def _chamar_llm_anthropic_pdf(
    pdf_bytes: bytes, user_text: str, system: str, modelo: str, max_tokens: int
) -> dict:
    """Chamada Anthropic com PDF nativo (base64 document)."""
    client = anthropic.Anthropic()
    content = [
        {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": base64.b64encode(pdf_bytes).decode(),
            },
        },
        {"type": "text", "text": user_text},
    ]
    message = client.messages.create(
        model=modelo,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": content}],
    )
    return {
        "text": message.content[0].text.strip(),
        "model": message.model,
        "input_tokens": int(message.usage.input_tokens),
        "output_tokens": int(message.usage.output_tokens),
    }


def _chamar_llm_openai_texto(
    texto_pdf: str, user_text: str, system: str, modelo: str, max_tokens: int,
    base_url: str | None = None, api_key_env: str = "OPENAI_API_KEY",
) -> dict:
    """Chamada OpenAI-compatible com texto extraído do PDF."""
    load_env()
    api_key = os.environ.get(api_key_env, "").strip() or None
    client = OpenAI(api_key=api_key, base_url=base_url or None)
    mensagem_user = f"{user_text}\n\n## Conteúdo do relatório (texto extraído do PDF)\n\n{texto_pdf}"
    resp = client.chat.completions.create(
        model=modelo,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": mensagem_user},
        ],
        max_completion_tokens=max_tokens,
    )
    usage = resp.usage
    return {
        "text": (resp.choices[0].message.content or "").strip(),
        "model": resp.model,
        "input_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
    }


def analisar_relatorio_ce(
    pdf_bytes: bytes,
    ce_nome: str,
    ano_letivo: str,
    provider: str,
    modelo: str,
    logger: AuditoriaLogger | None = None,
) -> str:
    """Envia o PDF do relatório de CE ao LLM e devolve o parecer em HTML.

    Args:
        pdf_bytes:  Bytes do PDF do relatório pedagógico.
        ce_nome:    Nome do ciclo de estudos (para o prompt).
        ano_letivo: Ano letivo (ex: "2024/25").
        provider:   Provider LLM: "anthropic", "openai" ou "iaedu".
        modelo:     ID do modelo a usar.
        logger:     Logger para registar progresso e metadados.

    Returns:
        String HTML com o parecer gerado pelo LLM.
    """
    load_env()
    _garantir_api_key(provider)
    system = _carregar_system_prompt()

    user_text = (
        f"Por favor, elabora um parecer ao relatório pedagógico do ciclo de estudos "
        f'"{ce_nome}", ano letivo {ano_letivo}, com base no PDF fornecido.'
    )

    max_tokens = 4096
    max_retries = int(os.environ.get("LLM_MAX_RETRIES", "3") or "3")
    base_wait = float(os.environ.get("LLM_RETRY_BASE_SECONDS", "2") or "2")
    max_wait = float(os.environ.get("LLM_RETRY_MAX_SECONDS", "20") or "20")

    t0 = time.monotonic()
    resp: dict | None = None
    tentativa = 0

    while True:
        try:
            if provider == "anthropic":
                resp = _chamar_llm_anthropic_pdf(pdf_bytes, user_text, system, modelo, max_tokens)
            elif provider == "iaedu":
                base_url = os.environ.get("IAEDU_ENDPOINT", "").strip() or None
                api_key_env = "IAEDU_API_KEY"
                texto_pdf = _pdf_to_text(pdf_bytes)
                resp = _chamar_llm_openai_texto(
                    texto_pdf, user_text, system, modelo, max_tokens,
                    base_url=base_url, api_key_env=api_key_env,
                )
            else:  # openai
                base_url = os.environ.get("OPENAI_BASE_URL", "").strip() or None
                texto_pdf = _pdf_to_text(pdf_bytes)
                resp = _chamar_llm_openai_texto(
                    texto_pdf, user_text, system, modelo, max_tokens,
                    base_url=base_url, api_key_env="OPENAI_API_KEY",
                )
            break
        except Exception as e:
            if tentativa >= max_retries or not _is_retryable_llm_error(e):
                raise
            espera = min(max_wait, base_wait * (2 ** tentativa)) + random.uniform(0, 0.4)
            time.sleep(espera)
            tentativa += 1

    duracao = time.monotonic() - t0

    if logger and resp:
        custo = _estimar_custo(resp["model"], resp["input_tokens"], resp["output_tokens"])
        logger.registar_llm(
            modelo=resp["model"],
            input_tokens=resp["input_tokens"],
            output_tokens=resp["output_tokens"],
            duracao=duracao,
            custo=custo,
        )

    return resp["text"] if resp else ""
