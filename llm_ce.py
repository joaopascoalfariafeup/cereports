"""
Módulo de análise de relatórios de Ciclos de Estudos (CEs) por LLM.

Função principal: analisar_relatorio_ce
  - Recebe PDF bytes do relatório pedagógico de um CE
  - Envia ao LLM com o system prompt de pareceres
  - Devolve o parecer em HTML
"""

from __future__ import annotations

import json
import os
import random
import re
import secrets
import time
import urllib.error
import urllib.request
from pathlib import Path

import anthropic
from openai import OpenAI

from bs4 import BeautifulSoup

from sigarra import load_env
from logger import AuditoriaLogger

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROMPTS_DIR = _SCRIPT_DIR / "prompts"

_SYSTEM_PROMPT: str | None = None


def _preprocess_relatorio_html(html: str) -> str:
    """Converte <input type="text" value="..."> em texto legível pelo LLM.

    Os relatórios do SIGARRA usam inputs readonly para dados de empregabilidade
    inseridos manualmente. O LLM não lê atributos value — converte para <span>.
    """
    soup = BeautifulSoup(html, "html.parser")
    for inp in soup.find_all("input", {"type": "text"}):
        val = (inp.get("value") or "").strip()
        span = soup.new_tag("span")
        span.string = val if val else "—"
        inp.replace_with(span)
    return str(soup)


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
        key = os.environ.get("IAEDU_API_KEY", "").strip()
        endpoint = os.environ.get("IAEDU_ENDPOINT", "").strip()
        canal = os.environ.get("IAEDU_ID_CANAL", "").strip()
        if not key or not endpoint or not canal:
            raise RuntimeError(
                "IAEDU_API_KEY, IAEDU_ENDPOINT e IAEDU_ID_CANAL são obrigatórios. "
                "Defina-os em .env antes de iniciar o servidor."
            )
        return
    key = os.environ.get(env_key, "").strip()
    if not key:
        raise RuntimeError(
            f"{env_key} não configurado. Defina-o em .env antes de iniciar o servidor."
        )


def _chamar_llm_iaedu_html(
    relatorio_html: str, user_text: str, system: str, modelo: str,
) -> dict:
    """Chamada IAedu via multipart/form-data com SSE streaming.

    A API IAedu não é OpenAI-compatible: usa POST multipart para um endpoint
    específico, com autenticação via header x-api-key, e devolve SSE.
    Variáveis de ambiente necessárias:
      IAEDU_ENDPOINT, IAEDU_API_KEY, IAEDU_ID_CANAL
    Opcionais:
      IAEDU_THREAD_ID, IAEDU_USER_INFO, IAEDU_USER_ID, IAEDU_USER_CONTEXT
    """
    load_env()
    endpoint = os.environ.get("IAEDU_ENDPOINT", "").strip()
    api_key = os.environ.get("IAEDU_API_KEY", "").strip()
    channel_id = os.environ.get("IAEDU_ID_CANAL", "").strip()
    thread_id = os.environ.get("IAEDU_THREAD_ID", "").strip() or secrets.token_urlsafe(16)
    user_info = os.environ.get("IAEDU_USER_INFO", "{}").strip() or "{}"
    user_id = os.environ.get("IAEDU_USER_ID", "").strip()
    user_context = os.environ.get("IAEDU_USER_CONTEXT", "").strip()

    # Combinar system prompt + user text (IAedu recebe uma única mensagem)
    full_message = f"{system.strip()}\n\n{user_text}\n\n## Conteúdo do relatório (HTML)\n\n{relatorio_html}"

    boundary = f"----iaedu-{secrets.token_hex(8)}"

    def _part(name: str, value: str) -> bytes:
        return (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n"
        ).encode("utf-8")

    body_chunks = [
        _part("channel_id", channel_id),
        _part("thread_id", thread_id),
        _part("user_info", user_info),
        _part("message", full_message),
    ]
    if modelo:
        body_chunks.append(_part("model", modelo))
    if user_id:
        body_chunks.append(_part("user_id", user_id))
    if user_context:
        body_chunks.append(_part("user_context", user_context))
    body_chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(body_chunks)

    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "x-api-key": api_key,
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Accept": "text/event-stream, application/json, text/plain, */*",
        },
        method="POST",
    )
    try:
        resp_raw = urllib.request.urlopen(req, timeout=180)
    except urllib.error.HTTPError as exc:
        body_err = ""
        try:
            body_err = exc.read().decode("utf-8", errors="replace")[:800]
        except Exception:
            pass
        raise urllib.error.HTTPError(
            exc.url, exc.code,
            f"{exc.reason}. Corpo: {body_err or '(vazio)'}",
            exc.headers, None,
        ) from exc

    with resp_raw:
        raw_text = resp_raw.read().decode("utf-8", errors="replace")

    def _extrair_texto_json(v) -> str:
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            for k in ("text", "content", "delta", "message", "response",
                      "output", "answer", "final_answer", "output_text", "result"):
                if k in v:
                    t = _extrair_texto_json(v[k])
                    if t:
                        return t
            return ""
        if isinstance(v, list):
            return "".join(_extrair_texto_json(i) for i in v)
        return ""

    def _parse_sse_payloads(txt: str) -> list[str]:
        payloads: list[str] = []
        data_lines: list[str] = []
        for raw_line in txt.splitlines():
            line = raw_line.rstrip("\r")
            if not line.strip():
                if data_lines:
                    payloads.append("\n".join(data_lines).strip())
                    data_lines = []
                continue
            s = line.strip()
            if s.startswith(":"):
                continue
            if s.startswith("data:"):
                data_lines.append(s[5:].strip())
        if data_lines:
            payloads.append("\n".join(data_lines).strip())
        return [p for p in payloads if p and p not in ("[DONE]", "__DONE__")]

    def _extract_from_event_obj(obj: dict) -> tuple[str, str]:
        if not isinstance(obj, dict):
            return "", ""
        tipo = str(obj.get("type", "")).strip().lower()
        conteudo = obj.get("content")
        if tipo == "token" and isinstance(conteudo, str):
            return conteudo, ""
        if tipo == "message":
            if isinstance(conteudo, str):
                return "", conteudo
            if isinstance(conteudo, dict):
                txt = _extrair_texto_json(conteudo)
                if txt:
                    return "", txt
        if isinstance(conteudo, str) and tipo not in ("start", "done"):
            return conteudo, ""
        return "", ""

    chunks: list[str] = []
    full_message_text = ""

    for payload in _parse_sse_payloads(raw_text):
        try:
            obj = json.loads(payload)
            tok, full = _extract_from_event_obj(obj)
            if tok:
                chunks.append(tok)
            if full:
                full_message_text = full
        except json.JSONDecodeError:
            chunks.append(payload)

    # Fallback: ndjson sem prefixo "data:"
    if not chunks and not full_message_text:
        for line in raw_text.splitlines():
            s = line.strip()
            if not s or not s.startswith("{"):
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                continue
            tok, full = _extract_from_event_obj(obj)
            if tok:
                chunks.append(tok)
            if full:
                full_message_text = full

    if full_message_text:
        texto = full_message_text.strip()
    elif chunks:
        texto = "".join(chunks).strip()
    else:
        try:
            obj = json.loads(raw_text)
            texto = _extrair_texto_json(obj).strip()
        except json.JSONDecodeError:
            texto = raw_text.strip()

    if not texto:
        snippet = re.sub(r"\s+", " ", raw_text).strip()[:700]
        raise ValueError(
            f"IAedu devolveu resposta sem texto útil. Snippet: {snippet or '(vazio)'}"
        )

    return {
        "text": texto,
        "model": modelo or os.environ.get("IAEDU_MODELO_ANALISE", "iaedu") or "iaedu",
        "input_tokens": 0,
        "output_tokens": 0,
    }


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


def _chamar_llm_anthropic_html(
    relatorio_html: str, user_text: str, system: str, modelo: str, max_tokens: int
) -> dict:
    """Chamada Anthropic com conteúdo HTML do relatório."""
    client = anthropic.Anthropic()
    mensagem_user = (
        f"{user_text}\n\n"
        f"## Conteúdo do relatório (HTML)\n\n{relatorio_html}"
    )
    message = client.messages.create(
        model=modelo,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": mensagem_user}],
    )
    return {
        "text": message.content[0].text.strip(),
        "model": message.model,
        "input_tokens": int(message.usage.input_tokens),
        "output_tokens": int(message.usage.output_tokens),
    }


def _chamar_llm_openai_html(
    relatorio_html: str, user_text: str, system: str, modelo: str, max_tokens: int,
    base_url: str | None = None, api_key_env: str = "OPENAI_API_KEY",
) -> dict:
    """Chamada OpenAI-compatible com conteúdo HTML do relatório."""
    load_env()
    api_key = os.environ.get(api_key_env, "").strip() or None
    client = OpenAI(api_key=api_key, base_url=base_url or None)
    mensagem_user = (
        f"{user_text}\n\n"
        f"## Conteúdo do relatório (HTML)\n\n{relatorio_html}"
    )
    resp = client.chat.completions.create(
        model=modelo,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": mensagem_user},
        ],
        max_tokens=max_tokens,
    )
    usage = resp.usage
    return {
        "text": (resp.choices[0].message.content or "").strip(),
        "model": resp.model,
        "input_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
    }


def analisar_relatorio_ce(
    relatorio_html: str,
    ce_nome: str,
    ano_letivo: str,
    provider: str,
    modelo: str,
    logger: AuditoriaLogger | None = None,
    pareceres_anteriores: str | None = None,
    perspetiva: str = "",
) -> str:
    """Envia o HTML do relatório de CE ao LLM e devolve o parecer em HTML.

    Args:
        relatorio_html: HTML limpo do relatório pedagógico (obtido via SIGARRA).
        ce_nome:        Nome do ciclo de estudos (para o prompt).
        ano_letivo:     Ano letivo (ex: "2024/25").
        provider:       Provider LLM: "anthropic", "openai" ou "iaedu".
        modelo:         ID do modelo a usar.
        logger:         Logger para registar progresso e metadados.

    Returns:
        String HTML com o parecer gerado pelo LLM.
    """
    load_env()
    _garantir_api_key(provider)
    system = _carregar_system_prompt()
    relatorio_html = _preprocess_relatorio_html(relatorio_html)

    _PERSPETIVA_LABELS = {
        "CC": "Conselho Científico (CC)",
        "CP": "Conselho Pedagógico (CP)",
        "CA": "Comissão de Acompanhamento (CA)",
        "DCE": "Diretor do Ciclo de Estudos — Auto-avaliação (DCE)",
    }
    perspetiva_label = _PERSPETIVA_LABELS.get((perspetiva or "").upper().strip(), "")

    user_text = (
        f"Por favor, elabora um parecer ao relatório pedagógico do ciclo de estudos "
        f'"{ce_nome}", ano letivo {ano_letivo}, com base no relatório fornecido.'
    )
    if perspetiva_label:
        user_text += f"\n\nPerspetiva do parecer: {perspetiva_label}"
    if pareceres_anteriores:
        user_text += (
            f"\n\n## Pareceres emitidos no relatório do ano letivo anterior\n\n"
            f"{pareceres_anteriores}"
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
                resp = _chamar_llm_anthropic_html(
                    relatorio_html, user_text, system, modelo, max_tokens
                )
            elif provider == "iaedu":
                resp = _chamar_llm_iaedu_html(
                    relatorio_html, user_text, system, modelo,
                )
            else:  # openai
                base_url = os.environ.get("OPENAI_BASE_URL", "").strip() or None
                resp = _chamar_llm_openai_html(
                    relatorio_html, user_text, system, modelo, max_tokens,
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

    texto = resp["text"] if resp else ""
    # Remover marcadores de bloco de código Markdown que o LLM por vezes inclui
    texto = re.sub(r"^```(?:html)?\s*\n?", "", texto.strip(), flags=re.IGNORECASE)
    texto = re.sub(r"\n?```\s*$", "", texto.strip())
    return texto.strip()
