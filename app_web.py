"""
Ponto de entrada Web para Assistente de Análise e Emissão de Pareceres sobre
Relatórios de Ciclos de Estudos (CEs) da FEUP.

- Login/password via formulário (SIGARRA)
- Upload de PDF do relatório de CE + seleção de ano letivo
- Análise por LLM em background
- Preview do parecer gerado com edição
"""

from __future__ import annotations

import base64
import html
import io
import json
import logging
import os
import re
import secrets
import threading
import time
import urllib.parse
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import urllib.request as _urllib_req

from flask import Flask, request, session as flask_session, redirect, url_for, Response, abort, send_file, make_response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from sigarra import SigarraSession, load_env
from logger import AuditoriaLogger
from ce_core import analisar_ce
from sigarra_ce import listar_ces_publicos, listar_relatorios_ce, obter_relatorio_ce_html, obter_cargos_docente, obter_pareceres_ano_anterior, submeter_parecer_sigarra, obter_parecer_atual_sigarra


# Carregar .env antes de ler variáveis WEB_* no arranque do módulo
load_env()

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

_SCRIPT_DIR = Path(__file__).resolve().parent

app = Flask(__name__)
app.secret_key = os.environ.get("WEB_SECRET_KEY") or secrets.token_hex(32)

_limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=[],
    storage_uri="memory://",
)

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("WEB_COOKIE_SECURE", "0").strip() == "1",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=8),
)

_output_dir_env = os.environ.get("AUDITORIA_OUTPUT_DIR", "").strip()
OUTPUT_DIR = (
    Path(_output_dir_env).resolve()
    if _output_dir_env
    else (_SCRIPT_DIR / "output").resolve()
)

# Armazenamento in-memory
_SESSOES: dict[str, SigarraSession] = {}
_SESSOES_LOCK = threading.Lock()

# Estados de autenticação federada em curso
_FED_STATES: dict[str, tuple[SigarraSession, str, str]] = {}
_FED_STATES_LOCK = threading.Lock()

# Sessão SIGARRA do servidor (partilhada; usada na autenticação Microsoft)
_SERVER_SESS: Optional[SigarraSession] = None
_SERVER_SESS_LOCK = threading.Lock()

# Estados OAuth Microsoft em curso: state → expires_at
_MS_STATES: dict[str, float] = {}
_MS_STATES_LOCK = threading.Lock()

# Estados OAuth OIDC em curso: state → expires_at
_OIDC_STATES: dict[str, float] = {}
_OIDC_STATES_LOCK = threading.Lock()

# OTPs de email em curso: email → {otp, codigo, expires}
_OTPS: dict[str, dict] = {}
_OTPS_LOCK = threading.Lock()

# Revisões de pareceres: persistidas em disco
REVIEWS_FILE = OUTPUT_DIR / "_reviews.json"
_REVIEWS_LOCK = threading.Lock()
REVIEW_TTL_DAYS = 30

_PERSPETIVA_LABELS_WEB = {
    "CC": "Conselho Científico (CC)",
    "CP": "Conselho Pedagógico (CP)",
    "CA": "Comissão de Acompanhamento (CA)",
    "DCE": "Diretor do Ciclo de Estudos — Auto-avaliação (DCE)",
}

# Configuração Microsoft OAuth (lida após load_env())
def _ms_config() -> dict:
    return {
        "tenant":        os.environ.get("MS_TENANT",       "up.pt"),
        "client_id":     os.environ.get("MS_CLIENT_ID",    ""),
        "client_secret": os.environ.get("MS_CLIENT_SECRET",""),
        "redirect_uri":  os.environ.get("MS_REDIRECT_URI", "https://ce.uc-reports.com/login/microsoft/callback"),
    }

# Configuração OIDC Keycloak UP (lida após load_env())
def _oidc_config() -> dict:
    return {
        "client_id":     os.environ.get("OIDC_CLIENT_ID",     ""),
        "client_secret": os.environ.get("OIDC_CLIENT_SECRET", ""),
        "redirect_uri":  os.environ.get("OIDC_REDIRECT_URI",  "https://ce.uc-reports.com/login/oidc/callback"),
        "auth_endpoint":    "https://open-id.up.pt/realms/sigarra/protocol/openid-connect/auth",
        "token_endpoint":   "https://open-id.up.pt/realms/sigarra/protocol/openid-connect/token",
        "userinfo_endpoint":"https://open-id.up.pt/realms/sigarra/protocol/openid-connect/userinfo",
    }


WEB_VERBOSIDADE = int(os.environ.get("WEB_VERBOSIDADE", "0"))
WEB_OUTPUT_RETENTION_HOURS = float(os.environ.get("WEB_OUTPUT_RETENTION_HOURS", "2"))
WEB_OUTPUT_MAX_GB = float(os.environ.get("WEB_OUTPUT_MAX_GB", "2"))
MAX_RUNNING_JOBS = int(os.environ.get("WEB_MAX_RUNNING_JOBS", "4"))
MAX_JOBS = int(os.environ.get("WEB_MAX_JOBS", "20"))
_SESSION_TIMEOUT_S = 8 * 3600
_JOB_TIMEOUT_S = int(os.environ.get("WEB_JOB_TIMEOUT_S", "600"))  # 10 min

_DRAINING_FILE = _SCRIPT_DIR / ".draining"

# ---------------------------------------------------------------------------
# Controlo de custos LLM por utilizador
# ---------------------------------------------------------------------------
WEB_COST_BYPASS_USERS: set[str] = {
    u.strip().lower()
    for u in os.environ.get("WEB_COST_BYPASS_USERS", "").split(",")
    if u.strip()
}
WEB_FREE_LLM_PROVIDERS_LIST: list[str] = []
for _p in os.environ.get("WEB_FREE_LLM_PROVIDERS", "iaedu").split(","):
    _v = _p.strip().lower()
    if _v and _v not in WEB_FREE_LLM_PROVIDERS_LIST:
        WEB_FREE_LLM_PROVIDERS_LIST.append(_v)
WEB_FREE_LLM_PROVIDERS_SET: set[str] = set(WEB_FREE_LLM_PROVIDERS_LIST)

_COSTS_FILE = OUTPUT_DIR / "_web_costs_monthly.json"
_USAGE_LOG_FILE = OUTPUT_DIR / "_web_usage_log.jsonl"
_COSTS_LOCK = threading.Lock()


def _month_key_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _load_costs_store() -> dict:
    if not _COSTS_FILE.exists():
        return {"month": _month_key_utc(), "users": {}}
    try:
        data = json.loads(_COSTS_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError
        month = str(data.get("month", "")).strip() or _month_key_utc()
        users = data.get("users", {})
        if not isinstance(users, dict):
            users = {}
        return {"month": month, "users": users}
    except Exception:
        return {"month": _month_key_utc(), "users": {}}


def _save_costs_store(data: dict) -> None:
    _COSTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _COSTS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(_COSTS_FILE)


def _user_cost_month(user_code: str) -> float:
    if not user_code:
        return 0.0
    month = _month_key_utc()
    with _COSTS_LOCK:
        data = _load_costs_store()
        if data.get("month") != month:
            data = {"month": month, "users": {}}
            _save_costs_store(data)
        try:
            return float(data.get("users", {}).get(user_code, 0.0))
        except (TypeError, ValueError):
            return 0.0


def _add_user_cost_month(user_code: str, usd: float) -> None:
    if not user_code or usd <= 0:
        return
    month = _month_key_utc()
    with _COSTS_LOCK:
        data = _load_costs_store()
        if data.get("month") != month:
            data = {"month": month, "users": {}}
        users = data.setdefault("users", {})
        try:
            atual = float(users.get(user_code, 0.0))
        except (TypeError, ValueError):
            atual = 0.0
        users[user_code] = round(atual + usd, 6)
        _save_costs_store(data)


def _append_usage_event(
    user_code: str, ce_nome: str, custo_usd: float,
    job_id: str, duracao_total_s: float,
    llm_provider: str = "", llm_modelo: str = "",
) -> None:
    evento = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
        "user_code": user_code,
        "ce_nome": ce_nome,
        "job_id": job_id,
        "custo_usd": round(float(custo_usd), 6),
        "duracao_total_s": round(float(duracao_total_s), 3),
        "llm_provider": (llm_provider or "").strip().lower(),
        "llm_modelo": (llm_modelo or "").strip(),
    }
    with _COSTS_LOCK:
        _USAGE_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _USAGE_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(evento, ensure_ascii=False) + "\n")


def _user_has_cost_bypass(user_code: str) -> bool:
    candidatos: set[str] = set()
    code = str(user_code or "").strip().lower()
    if code:
        candidatos.add(code)
        candidatos.add(f"up{code}")
    return bool(candidatos & WEB_COST_BYPASS_USERS)


def _max_usd_per_user_per_month() -> float:
    try:
        return float(os.environ.get("WEB_MAX_USD_PER_USER_PER_MONTH", "0") or "0")
    except ValueError:
        return 0.0


def _extrair_custo_estimado_valor(log_path: Path) -> float:
    try:
        txt = log_path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return 0.0
    matches = re.findall(r"Custo estimado:\s*\$([0-9]+(?:\.[0-9]+)?)", txt)
    total = 0.0
    for m in matches:
        try:
            total += float(m)
        except ValueError:
            continue
    return total


@dataclass
class Tarefa:
    job_id: str
    log_path: Path
    started_at: float
    ce_nome: str = ""
    ano_letivo: str = ""
    pv_id: str = ""
    cur_id: str = ""
    perspetiva: str = ""
    instrucoes: str = ""
    user_code: str = ""
    llm_provider: str = ""
    llm_modelo: str = ""
    run_dir: Path | None = None
    done: bool = False
    ok: bool = False
    error: str = ""


_JOBS: dict[str, Tarefa] = {}
_JOBS_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers: LLM providers / modelos
# ---------------------------------------------------------------------------

def _llm_provider_options() -> list[str]:
    raw = os.environ.get("WEB_LLM_PROVIDER_OPTIONS", "").strip()
    if raw:
        opts = [p.strip().lower() for p in raw.split(",") if p.strip()]
        if opts:
            return opts
    raw_models = os.environ.get("WEB_LLM_MODEL_OPTIONS_JSON", "").strip()
    if raw_models:
        try:
            data = json.loads(raw_models)
            if isinstance(data, dict):
                opts = [str(k).strip().lower() for k in data.keys() if str(k).strip()]
                if opts:
                    return opts
        except json.JSONDecodeError:
            pass
    return ["anthropic", "iaedu"]


def _llm_model_options_map() -> dict[str, list[str]]:
    raw = os.environ.get("WEB_LLM_MODEL_OPTIONS_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, list[str]] = {}
    for prov, models in data.items():
        limpos: list[str] = []
        if isinstance(models, list):
            limpos = [str(m).strip() for m in models if str(m).strip()]
        elif isinstance(models, str):
            limpos = [m.strip() for m in models.split(",") if m.strip()]
        if limpos:
            out[str(prov).strip().lower()] = limpos
    return out


def _default_modelo_por_provider(provider: str) -> str:
    p = (provider or "").strip().lower()
    if p == "openai":
        return os.environ.get("OPENAI_MODELO_ANALISE", "").strip() or "gpt-4o"
    if p == "iaedu":
        return os.environ.get("IAEDU_MODELO_ANALISE", "").strip() or "gpt-4o"
    return os.environ.get("ANTHROPIC_MODELO_ANALISE", "").strip() or "claude-opus-4-6"


def _format_model_cost(provider: str) -> str:
    if (provider or "").strip().lower() == "iaedu":
        return " — gratuito"
    return ""



# ---------------------------------------------------------------------------
# Helpers: formatação e UI
# ---------------------------------------------------------------------------

def _esc(v: object) -> str:
    return html.escape(str(v), quote=True)


def _format_ano_letivo_display(ano: object) -> str:
    s = str(ano or "").strip()
    if not s:
        return "-"
    if re.match(r"^\d{4}/\d{2,4}$", s):
        return s
    if re.match(r"^\d{4}$", s):
        y = int(s)
        return f"{y}/{(y + 1) % 100:02d}"
    return s


def _gera_lista_anos_letivos() -> list[dict]:
    """Gera lista de anos letivos recentes (corrente + 2 anteriores)."""
    now = datetime.now()
    ano_civil = now.year
    ano_inicio = ano_civil if now.month >= 9 else ano_civil - 1
    anos = []
    for y in range(ano_inicio, max(ano_inicio - 3, 2019) - 1, -1):
        label = f"{y}/{(y + 1) % 100:02d}"
        anos.append({"value": str(y), "label": label})
    return anos


def _ce_titulo_html(ce_nome: str, ano: str = "") -> str:
    nome_esc = _esc(ce_nome or "(sem nome)")
    ano_html = f'<span class="uc-ano-tag"> — {_esc(ano)}</span>' if ano else ""
    return (
        f'<p class="uc-card-title">'
        f'<span class="uc-sigla-tag">{nome_esc}</span>'
        f'{ano_html}'
        f'</p>'
    )


def _perspetivas_disponiveis(
    ce: dict,
    is_cc: bool,
    is_cp: bool,
    ca_set: set,
    director_set: set,
    is_admin: bool = False,
) -> list[dict]:
    """Devolve lista de {value, label} de perspetivas disponíveis para um CE.

    O diretor de um CE não pode emitir parecer sobre o próprio CE em nome
    de CC/CP/CA — apenas auto-avaliação (DCE).
    Admins têm acesso a todas as perspetivas para qualquer CE.
    """
    tipo = ce.get("tipo", "")
    cur_id = ce.get("cur_id", "")
    if is_admin:
        persp = [
            {"value": "CC", "label": "Conselho Científico"},
            {"value": "DCE", "label": "Diretor (auto-avaliação)"},
        ]
        if tipo in ("L", "M"):
            persp.insert(1, {"value": "CP", "label": "Conselho Pedagógico"})
        if tipo == "D":
            persp.append({"value": "CA", "label": "Comissão de Acompanhamento"})
        return persp
    # Diretor deste CE: só auto-avaliação (não pode emitir parecer de órgão
    # sobre um relatório que ele próprio elaborou)
    if cur_id in director_set:
        return [{"value": "DCE", "label": "Diretor (auto-avaliação)"}]
    persp: list[dict] = []
    if is_cc:
        persp.append({"value": "CC", "label": "Conselho Científico"})
    if is_cp and tipo in ("L", "M"):
        persp.append({"value": "CP", "label": "Conselho Pedagógico"})
    if cur_id in ca_set and tipo == "D":
        persp.append({"value": "CA", "label": "Comissão de Acompanhamento"})
    return persp


# ---------------------------------------------------------------------------
# CSRF
# ---------------------------------------------------------------------------

def _new_csrf_token() -> str:
    token = secrets.token_urlsafe(24)
    flask_session["csrf_token"] = token
    return token


def _get_csrf_token() -> str:
    token = flask_session.get("csrf_token")
    if not token:
        token = _new_csrf_token()
    return token


def _require_csrf() -> None:
    sent = request.form.get("csrf_token", "")
    expected = flask_session.get("csrf_token", "")
    if not expected or not sent or sent != expected:
        abort(400, "CSRF token inválido")


# ---------------------------------------------------------------------------
# Sessão SIGARRA
# ---------------------------------------------------------------------------

def _get_sigarra_session() -> Optional[SigarraSession]:
    sid = flask_session.get("sigarra_sid")
    if not sid:
        return None
    with _SESSOES_LOCK:
        return _SESSOES.get(sid)


def _set_sigarra_session(sess: SigarraSession) -> None:
    sid = secrets.token_urlsafe(16)
    sess._created_at = time.time()
    with _SESSOES_LOCK:
        _SESSOES[sid] = sess
    flask_session["sigarra_sid"] = sid


def _clear_sigarra_session() -> None:
    sid = flask_session.pop("sigarra_sid", None)
    if sid:
        with _SESSOES_LOCK:
            _SESSOES.pop(sid, None)


def _get_server_session() -> SigarraSession:
    """Devolve sessão SIGARRA do servidor, autenticando na primeira chamada."""
    global _SERVER_SESS
    with _SERVER_SESS_LOCK:
        if _SERVER_SESS is not None and _SERVER_SESS.autenticado:
            return _SERVER_SESS
        login    = os.environ.get("SIGARRA_SERVER_LOGIN",    "")
        password = os.environ.get("SIGARRA_SERVER_PASSWORD", "")
        if not login or not password:
            raise RuntimeError("SIGARRA_SERVER_LOGIN/PASSWORD não configurados no .env")
        sess = SigarraSession()
        sess.autenticar(login, password)
        _SERVER_SESS = sess
        return _SERVER_SESS


def _codigo_de_email_ms(email: str) -> Optional[str]:
    """Extrai código SIGARRA de email Microsoft institucional UP.

    up210006@up.pt          → '210006'
    up202206705@edu.fe.up.pt → '202206705'
    Devolve None se o padrão não for reconhecido.
    """
    local = email.split("@")[0].lower()
    m = re.match(r"^up(\d+)$", local)
    return m.group(1) if m else None


def _is_job_owner(job: Tarefa, sess: SigarraSession) -> bool:
    owner = (job.user_code or "").strip()
    current = (sess.codigo_pessoal or "").strip()
    return bool(owner and current and owner == current)


def _admin_codes() -> set[str]:
    load_env()
    raw = os.environ.get("ADMIN_CODES", "").strip()
    return {c.strip() for c in raw.split(",") if c.strip()} if raw else set()


def _is_admin(sess: SigarraSession) -> bool:
    return bool(sess.codigo_pessoal and sess.codigo_pessoal in _admin_codes())


def _load_reviews() -> list[dict]:
    try:
        if REVIEWS_FILE.exists():
            return json.loads(REVIEWS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def _save_reviews(reviews: list[dict]) -> None:
    REVIEWS_FILE.parent.mkdir(parents=True, exist_ok=True)
    REVIEWS_FILE.write_text(json.dumps(reviews, ensure_ascii=False, indent=2), encoding="utf-8")


def _create_review(job: "Tarefa", reviewer_code: str, reviewer_email: str, mensagem: str,
                   owner_nome: str = "", reviewer_nome: str = "") -> str:
    token = secrets.token_urlsafe(24)
    now = time.time()
    owner_code = job.user_code or ""
    record = {
        "token": token,
        "job_id": job.job_id,
        "run_dir": str(job.run_dir),
        "ce_nome": job.ce_nome,
        "ano_letivo": job.ano_letivo,
        "perspetiva": job.perspetiva,
        "pv_id": job.pv_id,
        "cur_id": job.cur_id,
        "owner_code": owner_code,
        "owner_email": f"up{owner_code}@up.pt" if owner_code else "",
        "owner_nome": owner_nome,
        "reviewer_code": reviewer_code,
        "reviewer_email": reviewer_email,
        "reviewer_nome": reviewer_nome,
        "mensagem": mensagem,
        "estado": "pendente",
        "criado_em": now,
        "expira_em": now + REVIEW_TTL_DAYS * 86400,
    }
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
        reviews.append(record)
        _save_reviews(reviews)
    return token


def _get_review(token: str) -> dict | None:
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
    now = time.time()
    for r in reviews:
        if r.get("token") == token and r.get("expira_em", 0) > now:
            return r
    return None


def _reviews_for_user(code: str) -> list[dict]:
    """Devolve revisões pendentes (não concluídas) para o utilizador."""
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
    now = time.time()
    return [
        r for r in reviews
        if r.get("reviewer_code") == code
        and r.get("expira_em", 0) > now
        and r.get("estado") != "concluido"
    ]


def _update_review_fields(token: str, **fields) -> None:
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
        for r in reviews:
            if r.get("token") == token:
                r.update(fields)
                break
        _save_reviews(reviews)


def _conclude_review(token: str) -> None:
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
        for r in reviews:
            if r.get("token") == token:
                r["estado"] = "concluido"
                r["concluido_em"] = time.time()
                break
        _save_reviews(reviews)


def _prune_reviews() -> None:
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
        active = [r for r in reviews if r.get("expira_em", 0) > time.time()]
        if len(active) != len(reviews):
            _save_reviews(active)


def _active_review_run_dirs() -> set[str]:
    with _REVIEWS_LOCK:
        reviews = _load_reviews()
    now = time.time()
    return {r["run_dir"] for r in reviews if r.get("expira_em", 0) > now and "run_dir" in r}


def _reviewer_tem_permissao(reviewer_code: str, cur_id: str, perspetiva: str) -> bool:
    """Verifica se reviewer_code tem permissão para a perspetiva/CE, via sessão servidor."""
    if reviewer_code in _admin_codes():
        return True
    try:
        server_sess = _get_server_session()
        cargos = obter_cargos_docente(server_sess, reviewer_code)
    except Exception:
        return False
    if perspetiva == "CC":
        return bool(cargos.get("is_cc"))
    elif perspetiva == "CP":
        return bool(cargos.get("is_cp") or cargos.get("is_cc"))
    elif perspetiva == "CA":
        ca_ids = {c["cur_id"] for c in cargos.get("cac_cursos", [])}
        return cur_id in ca_ids
    elif perspetiva == "DCE":
        director_ids = {d["cur_id"] for d in cargos.get("director_cursos", [])}
        return cur_id in director_ids
    return False


def _send_review_email(reviewer_email: str, ce_nome: str, ano_letivo: str, perspetiva: str,
                        owner_code: str, owner_nome: str, owner_email: str,
                        token: str, mensagem: str) -> None:
    api_key = _resend_api_key()
    from_addr = _resend_from()
    perspetiva_label = _PERSPETIVA_LABELS_WEB.get(perspetiva, perspetiva)
    link = url_for("revisao_get", token=token, _external=True)
    owner_display = f"{html.escape(owner_nome)} ({html.escape(owner_code)})" if owner_nome else html.escape(owner_code)
    msg_block = (
        f"<p><b>Mensagem:</b><br>{html.escape(mensagem)}</p>"
        if mensagem else ""
    )
    body_html = f"""
<p>{owner_display} enviou-lhe um pedido de revisão de parecer gerado com apoio de IA.</p>
<ul>
  <li><b>Ciclo de estudos:</b> {html.escape(ce_nome)}</li>
  <li><b>Ano letivo:</b> {html.escape(ano_letivo)}</li>
  <li><b>Perspetiva:</b> {html.escape(perspetiva_label)}</li>
</ul>
{msg_block}
<p><a href="{link}">Clique aqui para aceder ao parecer e rever</a></p>
<p style="color:#888;font-size:0.9em;">O link é válido por {REVIEW_TTL_DAYS} dias. Requer autenticação na aplicação.</p>
"""
    mail_payload: dict = {
        "from": from_addr,
        "to": [reviewer_email],
        "subject": f"Revisão de parecer — {ce_nome} {ano_letivo}",
        "html": body_html,
    }
    if owner_email:
        mail_payload["cc"] = [owner_email]
    payload = json.dumps(mail_payload).encode()
    req = _urllib_req.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "ucreports/1.0",
        },
        method="POST",
    )
    with _urllib_req.urlopen(req, timeout=10) as resp:
        status = resp.status
    if status not in (200, 201):
        raise RuntimeError(f"Resend HTTP {status}")


def _send_conclusion_email(owner_email: str, owner_nome: str, owner_code: str,
                            reviewer_code: str, reviewer_nome: str,
                            ce_nome: str, ano_letivo: str, perspetiva: str) -> None:
    if not owner_email:
        return
    api_key = _resend_api_key()
    if not api_key:
        return
    from_addr = _resend_from()
    perspetiva_label = _PERSPETIVA_LABELS_WEB.get(perspetiva, perspetiva)
    reviewer_display = f"{html.escape(reviewer_nome)} ({html.escape(reviewer_code)})" if reviewer_nome else html.escape(reviewer_code)
    body_html = f"""
<p>{reviewer_display} concluiu a revisão do seguinte parecer:</p>
<ul>
  <li><b>Ciclo de estudos:</b> {html.escape(ce_nome)}</li>
  <li><b>Ano letivo:</b> {html.escape(ano_letivo)}</li>
  <li><b>Perspetiva:</b> {html.escape(perspetiva_label)}</li>
</ul>
<p style="color:#888;font-size:0.9em;">O parecer revisto foi guardado pelo revisor.</p>
"""
    payload = json.dumps({
        "from": from_addr,
        "to": [owner_email],
        "subject": f"Revisão concluída — {ce_nome} {ano_letivo}",
        "html": body_html,
    }).encode()
    req = _urllib_req.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "ucreports/1.0",
        },
        method="POST",
    )
    with _urllib_req.urlopen(req, timeout=10) as resp:
        status = resp.status
    if status not in (200, 201):
        raise RuntimeError(f"Resend HTTP {status}")


def _get_impersonated_code() -> str | None:
    return flask_session.get("impersonated_code") or None


def _effective_codigo(sess: SigarraSession) -> str:
    return _get_impersonated_code() or sess.codigo_pessoal or ""


# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

def _reap_stuck_jobs() -> None:
    now = time.time()
    with _JOBS_LOCK:
        for job in list(_JOBS.values()):
            if not job.done and (now - job.started_at) > _JOB_TIMEOUT_S:
                job.ok = False
                job.error = f"Timeout ({_JOB_TIMEOUT_S}s) — a tarefa foi cancelada."
                job.done = True


def _prune_output_dir() -> None:
    """Remove output dirs mais antigos que o tempo de retenção configurado."""
    _reap_stuck_jobs()
    _prune_reviews()
    cutoff = time.time() - WEB_OUTPUT_RETENTION_HOURS * 3600
    if not OUTPUT_DIR.is_dir():
        return
    protected = _active_review_run_dirs()
    for entry in OUTPUT_DIR.iterdir():
        if entry.is_dir() and str(entry) not in protected and entry.stat().st_mtime < cutoff:
            try:
                import shutil
                shutil.rmtree(entry, ignore_errors=True)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Hooks Flask
# ---------------------------------------------------------------------------

@app.before_request
def _before_request():
    flask_session.permanent = True


@app.after_request
def _secure_headers(resp: Response):
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "base-uri 'none'; "
        "object-src 'none'; "
        "form-action 'self'; "
        "img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self'; "
        "connect-src 'self'; "
        "frame-ancestors 'none';"
    )
    return resp


# ---------------------------------------------------------------------------
# Layout / CSS
# ---------------------------------------------------------------------------

_STEPPER_LABELS = ["Seleção", "Geração", "Revisão", "Submissão"]


def _stepper_html(step: int, logout_url: str = "") -> str:
    if step < 1:
        return ""
    parts: list[str] = []
    for i, label in enumerate(_STEPPER_LABELS, 1):
        if i < step:
            cls = "stepper-step done"
            num = "&#10003;"
        elif i == step:
            cls = "stepper-step active"
            num = str(i)
        else:
            cls = "stepper-step"
            num = str(i)
        if i > 1:
            parts.append('<span class="stepper-arrow">&#8250;</span>')
        if i == 1 and i < step:
            ces_url = url_for("ces")
            parts.append(
                f'<a class="stepper-step done stepper-link" href="{ces_url}">'
                f'<span class="stepper-num">{num}</span>'
                f'<span class="stepper-label">{label}</span>'
                f'</a>'
            )
        else:
            parts.append(
                f'<span class="{cls}">'
                f'<span class="stepper-num">{num}</span>'
                f'<span class="stepper-label">{label}</span>'
                f'</span>'
            )
    sair_html = ""
    if logout_url:
        sair_html = f'<a class="stepper-sair" href="{_esc(logout_url)}">Sair</a>'
    return (
        '<div class="stepper-bar">'
        '<div class="stepper">' + "".join(parts) + '</div>'
        + sair_html +
        '</div>'
    )


def _page(title: str, body: str, step: int = 0) -> str:
    logout_url = url_for("logout") if step >= 1 else ""
    stepper = _stepper_html(step, logout_url=logout_url)
    return f"""<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Assistente de Apoio à Elaboração de Parecers sobre Relatórios de Ciclos de Estudos</title>
  <link rel="icon" type="image/svg+xml" href="{url_for('favicon_svg')}">
  <style>
    :root {{
      --bg: #f3f4f6;
      --panel: #ffffff;
      --fg: #111827;
      --muted: #6b7280;
      --line: #d1d5db;
      --ok: #16a34a;
      --warn: #d97706;
      --err: #dc2626;
      --accent: #2563eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: Inter, Segoe UI, Arial, sans-serif;
      margin: 0;
      color: var(--fg);
      background: var(--bg);
      line-height: 1.45;
    }}
    .container {{ max-width: 980px; margin: 24px auto 44px; padding: 0 18px; }}
    .app-header {{ margin: 0 0 14px; }}
    .app-brandrow {{ display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
    .app-brand {{ font-size: 18px; font-weight: 800; letter-spacing: .2px; }}
    .app-subtitle {{ margin-top: 2px; color: var(--muted); font-size: 13px; }}
    .page-title {{ margin: 0 0 16px; font-size: 20px; font-weight: 750; letter-spacing: .1px; }}
    h3 {{ margin: 0 0 10px; font-size: 16px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px 15px;
      margin: 10px 0;
      overflow: hidden;
    }}
    .row {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
    .form-row-inline {{ display:flex; align-items:center; gap:10px; margin-top:10px; min-width:0; }}
    .form-row-inline label {{ flex-shrink:0; min-width:150px; margin:0; }}
    .form-row-inline select,
    .form-row-inline input[type="text"],
    .form-row-inline input[type="file"] {{ flex:1; min-width:0; max-width:600px; padding-left:7px; padding-right:7px; }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .mutedsmall {{ color: var(--muted); font-size: 12px; }}
    label {{ color: var(--muted); }}
    input, select {{
      padding: 9px 11px;
      font-size: 14px;
      color: var(--fg);
      background: #ffffff;
      border: 1px solid var(--line);
      border-radius: 9px;
      outline: none;
    }}
    input:focus, select:focus {{ border-color: var(--accent); box-shadow: 0 0 0 2px rgba(59,130,246,.25); }}
    button, .btn {{
      padding: 9px 13px;
      font-size: 14px;
      cursor: pointer;
      background: var(--accent);
      color: #fff;
      border: 0;
      border-radius: 9px;
      font-weight: 650;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      line-height: 1.1;
    }}
    button:hover, .btn:hover {{ filter: brightness(1.05); text-decoration: none; }}
    .btn-secondary {{
      background: #fff;
      color: var(--accent);
      border: 1px solid var(--line);
    }}
    .btn-secondary:hover {{ filter: none; background: #f9fafb; }}
    .input-with-suffix {{ display:flex; align-items:center; gap:8px; width:320px; max-width:100%; }}
    .input-with-suffix input {{ width:100%; }}
    .input-suffix {{ color: var(--muted); font-size: 13px; white-space: nowrap; }}
    pre {{
      white-space: pre-wrap;
      background: #f9fafb;
      color: #1f2937;
      font-size: 0.875rem;
      line-height: 1.5;
      padding: 12px;
      border-radius: 10px;
      overflow-x: auto;
      height: 18em;
      overflow-y: auto;
      border: 1px solid var(--line);
    }}
    a {{ color: #1d4ed8; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    code {{ background: #f3f4f6; border: 1px solid var(--line); padding: 2px 6px; border-radius: 6px; }}
    .pill {{ display:inline-block; padding: 3px 10px; border-radius: 999px; background:#f3f4f6; border:1px solid var(--line); color: var(--muted); font-size: 12px; font-weight: 500; }}
    .status-ok {{ color: #15803d; }}
    .status-err {{ color: #b91c1c; }}
    .status-run {{ color: #1d4ed8; }}
    .status-warn {{ background:#fef3c7; border:1px solid #f59e0b; color:#92400e; border-radius:6px; padding:8px 12px; margin-bottom:10px; font-size:0.95em; }}
    .uc-card-title {{
      margin: 0;
      display: flex;
      align-items: baseline;
      min-width: 0;
      overflow: hidden;
      gap: 0;
    }}
    .uc-sigla-tag {{ font-weight: normal; flex-shrink: 0; white-space: nowrap; }}
    .uc-ano-tag {{ font-weight: normal; flex-shrink: 0; white-space: nowrap; }}
    .stepper-bar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin: 0 0 18px;
      gap: 12px;
    }}
    .stepper-sair {{
      font-size: 13px;
      color: var(--muted);
      text-decoration: none;
      white-space: nowrap;
      padding: 4px 10px;
      border: 1px solid var(--line);
      border-radius: 7px;
      background: var(--panel);
    }}
    .stepper-sair:hover {{ background: #f3f4f6; text-decoration: none; }}
    .stepper {{
      display: flex;
      align-items: center;
      gap: 0;
      font-size: 13px;
      color: var(--muted);
    }}
    .stepper-step {{
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .stepper-num {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 22px; height: 22px;
      border-radius: 50%;
      border: 1.5px solid var(--line);
      font-size: 11px;
      font-weight: 600;
      color: var(--muted);
      background: var(--bg);
      flex-shrink: 0;
    }}
    .stepper-step.active .stepper-num {{
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
    }}
    .stepper-step.done .stepper-num {{
      background: var(--ok);
      border-color: var(--ok);
      color: #fff;
    }}
    .stepper-step.active .stepper-label {{ color: var(--fg); font-weight: 600; }}
    .stepper-arrow {{ margin: 0 8px; color: var(--line); font-size: 16px; }}
    .stepper-link {{ text-decoration: none !important; }}
    .navbar {{ display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap; }}
    .navbar-left, .navbar-right {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .preview-html {{
      background: #f9fafb;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      font-size: 13px;
    }}
    .preview-html[contenteditable="true"] {{
      background: #fff;
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(59,130,246,.15);
      outline: none;
      min-height: 80px;
    }}
    .preview-html p {{ margin: 0 0 10px; }}
    .preview-html p:last-child {{ margin-bottom: 0; }}
    .preview-html h3 {{ margin: 14px 0 6px; font-size: 15px; }}
    .preview-html ul {{ margin: 4px 0 10px 18px; padding: 0; }}
    .preview-html li {{ margin: 4px 0; }}
    .editable-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }}
    .editable-header h3 {{ margin: 0; }}
    .btn-edit {{
      background: transparent;
      color: var(--muted);
      border: 1px solid var(--line);
      padding: 3px 10px;
      font-size: 12px;
      font-weight: 500;
      border-radius: 6px;
      cursor: pointer;
    }}
    .btn-edit:hover {{ background: #f3f4f6; color: var(--fg); }}
    .btn-edit.editing {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }}
    .btn-cancel-edit {{
      display: none;
      background: transparent;
      color: var(--muted);
      border: 1px solid var(--line);
      padding: 3px 10px;
      font-size: 12px;
      font-weight: 500;
      border-radius: 6px;
      cursor: pointer;
    }}
    .btn-cancel-edit.visible {{ display: inline-flex !important; }}
    .btn-cancel-edit:hover {{ background: #f3f4f6; color: var(--fg); }}
    .edit-counter {{
      display: none;
      font-size: 12px;
      color: var(--muted);
      padding: 2px 6px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
    }}
    .edit-counter.visible {{ display: inline !important; }}
    .edit-counter.over-limit {{ color: var(--err); border-color: var(--err); background: #fef2f2; }}
    ul {{ margin: 8px 0 0 18px; padding: 0; }}
    li {{ margin: 4px 0; }}
    .edit-toolbar {{
      display: none;
      flex-wrap: wrap;
      gap: 2px;
      padding: 6px 8px;
      background: var(--panel);
      border: 1px solid var(--accent);
      border-bottom: none;
      border-radius: 12px 12px 0 0;
    }}
    .edit-toolbar.visible {{ display: flex; }}
    .edit-toolbar + .preview-html[contenteditable="true"] {{ border-radius: 0 0 12px 12px; }}
    .edit-toolbar button {{
      background: transparent;
      border: 1px solid transparent;
      padding: 2px 7px;
      font-size: 13px;
      cursor: pointer;
      border-radius: 4px;
      color: var(--fg);
      min-width: 28px;
      font-weight: normal;
    }}
    .edit-toolbar button:hover {{ background: #e5e7eb; border-color: var(--line); }}
    .edit-toolbar .sep {{ width: 1px; background: var(--line); margin: 2px 4px; align-self: stretch; }}
  </style>
</head>
<body>
<div class="container">
  <header class="app-header">
    <div class="app-brandrow">
      <span class="app-brand">Assistente de Apoio à Elaboração de Pareceres sobre Relatórios de Ciclos de Estudos</span>
      <span class="pill">Piloto</span>
    </div>
    <div class="app-subtitle">FEUP · Melhoria Contínua</div>
  </header>
  {stepper}
  {f'<h1 class="page-title">{_esc(title)}</h1>' if not step else ''}
  {body}
</div>
<script src="{url_for('static_app_js')}"></script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Favicon e JS
# ---------------------------------------------------------------------------

@app.get("/favicon.svg")
def favicon_svg():
    svg = """<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>
  <defs>
    <linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>
      <stop offset='0%' stop-color='#2563eb'/>
      <stop offset='100%' stop-color='#1d4ed8'/>
    </linearGradient>
  </defs>
  <rect x='4' y='4' width='56' height='56' rx='12' fill='url(#g)'/>
  <text x='32' y='39' text-anchor='middle' font-size='18' font-family='Arial, sans-serif' fill='white' font-weight='700'>CE</text>
</svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.get("/static/app.js")
def static_app_js():
    js = r"""
// app.js — CE Reports UI helper

function _byId(id) { return document.getElementById(id); }

function setupLogin() {
  const form = _byId('login-form');
  if (!form) return;
  form.addEventListener('submit', () => {
    const btn = _byId('btn-login');
    if (btn) {
      btn.disabled = true;
      btn.textContent = 'A autenticar...';
    }
  });
}

function setupProgressSSE() {
  const pre = _byId('console');
  if (!pre) return;
  const eventsUrl = pre.dataset.eventsUrl;
  if (!eventsUrl) return;
  const shouldReloadOnDone = (pre.dataset.shouldReloadOnDone || 'false') === 'true';
  let doneHandled = false;

  const es = new EventSource(eventsUrl);
  es.onmessage = (ev) => {
    if (!ev.data) return;
    if (ev.data === '__DONE__') {
      es.close();
      if (shouldReloadOnDone && !doneHandled) {
        doneHandled = true;
        const savedLog = pre.textContent;
        fetch(window.location.href)
          .then(r => r.text())
          .then(html => {
            const doc = new DOMParser().parseFromString(html, 'text/html');
            const newStepper = doc.querySelector('.stepper-bar');
            const curStepper = document.querySelector('.stepper-bar');
            if (newStepper && curStepper) curStepper.outerHTML = newStepper.outerHTML;
            const newPB = doc.getElementById('progress-body');
            const curPB = document.getElementById('progress-body');
            if (newPB && curPB) {
              curPB.outerHTML = newPB.outerHTML;
              const newPre = document.getElementById('console');
              if (newPre && savedLog) newPre.textContent = savedLog;
            }
          })
          .catch(() => window.location.reload());
      }
      return;
    }
    pre.textContent += ev.data;
    pre.scrollTop = pre.scrollHeight;
  };
  es.onerror = () => { es.close(); };
}

function setupEditableBlocks() {
  const MAX_CHARS = 10000;
  document.querySelectorAll('.editable-header[data-editable-id]').forEach(header => {
    const id = header.dataset.editableId;
    const block = document.querySelector('.preview-html[data-field="' + id + '"]');
    const btnEdit = header.querySelector('.btn-edit');
    const btnCancel = header.querySelector('.btn-cancel-edit');
    const counter = header.querySelector('.edit-counter');
    const hidden = document.getElementById('field_' + id);
    const toolbar = document.getElementById('toolbar-' + id);
    const form = hidden ? hidden.closest('form') : null;
    const submitBtn = form ? form.querySelector('button[type="submit"]') : null;
    if (!block || !btnEdit) return;

    let original = block.innerHTML;

    function updateCounter() {
      if (!counter) return;
      const len = block.innerHTML.length;
      counter.textContent = len + ' / ' + MAX_CHARS;
      if (len > MAX_CHARS) {
        counter.classList.add('over-limit');
        if (submitBtn) { submitBtn.disabled = true; submitBtn.title = 'O parecer excede o limite de ' + MAX_CHARS + ' carateres.'; }
      } else {
        counter.classList.remove('over-limit');
        if (submitBtn) { submitBtn.disabled = false; submitBtn.title = ''; }
      }
    }

    btnEdit.addEventListener('click', () => {
      if (block.contentEditable === 'true') {
        // Guardar
        if (hidden) hidden.value = block.innerHTML;
        block.contentEditable = 'false';
        btnEdit.textContent = 'Editar';
        btnEdit.classList.remove('editing');
        if (btnCancel) { btnCancel.classList.remove('visible'); }
        if (counter) { counter.classList.remove('visible'); }
        if (toolbar) { toolbar.classList.remove('visible'); }
        if (submitBtn) { submitBtn.disabled = false; submitBtn.title = ''; }
      } else {
        original = block.innerHTML;
        block.contentEditable = 'true';
        document.execCommand('defaultParagraphSeparator', false, 'p');
        block.focus();
        btnEdit.textContent = 'Guardar';
        btnEdit.classList.add('editing');
        if (btnCancel) { btnCancel.classList.add('visible'); }
        if (counter) { counter.classList.add('visible'); updateCounter(); }
        if (toolbar) { toolbar.classList.add('visible'); }
      }
    });

    if (btnCancel) {
      btnCancel.addEventListener('click', () => {
        block.innerHTML = original;
        block.contentEditable = 'false';
        btnEdit.textContent = 'Editar';
        btnEdit.classList.remove('editing');
        btnCancel.classList.remove('visible');
        if (counter) { counter.classList.remove('visible'); }
        if (toolbar) { toolbar.classList.remove('visible'); }
        if (hidden) hidden.value = original;
        if (submitBtn) { submitBtn.disabled = false; submitBtn.title = ''; }
      });
    }

    if (counter) {
      block.addEventListener('input', updateCounter);
    }

    // Garantir que o conteúdo editado é guardado mesmo se o form for submetido em modo edição
    if (form) {
      form.addEventListener('submit', () => {
        if (block.contentEditable === 'true' && hidden) {
          hidden.value = block.innerHTML;
        }
      });
    }
  });

  // Botões de formatação da toolbar
  document.querySelectorAll('.edit-toolbar button[data-cmd]').forEach((tbBtn) => {
    tbBtn.addEventListener('click', (e) => {
      e.preventDefault();
      document.execCommand(tbBtn.dataset.cmd, false, null);
    });
  });
}

function setupCeYearLoader() {
  const ceSelect = _byId('ce_nome');
  const anoSelect = _byId('ano_letivo');
  if (!ceSelect || !anoSelect) return;

  function getFallbackAnos() {
    try {
      return JSON.parse(anoSelect.dataset.fallbackAnos || '[]');
    } catch(e) { return []; }
  }

  function syncPvId() {
    const pvIdInput = _byId('pv_id');
    if (!pvIdInput) return;
    const opt = anoSelect.options[anoSelect.selectedIndex];
    pvIdInput.value = (opt && opt.dataset.pvId) ? opt.dataset.pvId : '';
  }

  function populateAnos(anos) {
    const currentVal = anoSelect.value;
    anoSelect.innerHTML = '';
    anos.forEach(function(a) {
      const opt = document.createElement('option');
      opt.value = a.value;
      opt.textContent = a.label;
      if (a.pvId) opt.dataset.pvId = a.pvId;
      if (a.pv_id) opt.dataset.pvId = a.pv_id;
      if (a.value === currentVal) opt.selected = true;
      anoSelect.appendChild(opt);
    });
    // Default: selecionar o mais recente (primeiro da lista)
    if (!anoSelect.value && anos.length > 0) {
      anoSelect.value = anos[0].value;
    }
    syncPvId();
  }

  function loadYears(curId) {
    if (!curId) {
      populateAnos(getFallbackAnos());
      return;
    }
    fetch('/api/relatorios_ce/' + encodeURIComponent(curId))
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var anos = (data.anos && data.anos.length > 0) ? data.anos : getFallbackAnos();
        populateAnos(anos);
      })
      .catch(function() { populateAnos(getFallbackAnos()); });
  }

  function updatePerspetivas() {
    const perspSelect = _byId('perspetiva');
    const perspRow = _byId('perspetiva-row');
    if (!perspSelect) return;
    const opt = ceSelect.options[ceSelect.selectedIndex];
    var persp = [];
    try { persp = JSON.parse((opt && opt.dataset.perspetivas) || '[]'); } catch(e) {}
    const prev = perspSelect.value;
    perspSelect.innerHTML = '';
    if (persp.length === 0) {
      if (perspRow) perspRow.style.display = 'none';
      return;
    }
    if (perspRow) perspRow.style.display = '';
    persp.forEach(function(p) {
      const o = document.createElement('option');
      o.value = p.value;
      o.textContent = p.label;
      if (p.value === prev) o.selected = true;
      perspSelect.appendChild(o);
    });
    if (!perspSelect.value && persp.length > 0) perspSelect.value = persp[0].value;
  }

  const curIdHidden = _byId('cur_id_hidden');
  function onCeChange() {
    const opt = ceSelect.options[ceSelect.selectedIndex];
    const curId = opt ? (opt.dataset.curId || '') : '';
    if (curIdHidden) curIdHidden.value = curId;
    loadYears(curId);
    updatePerspetivas();
  }

  ceSelect.addEventListener('change', onCeChange);
  anoSelect.addEventListener('change', syncPvId);

  // Carregar imediatamente ao entrar na página
  onCeChange();
}

document.addEventListener('DOMContentLoaded', () => {
  setupLogin();
  setupProgressSSE();
  setupEditableBlocks();
  setupCeYearLoader();
});
"""
    r = Response(js, mimetype="application/javascript")
    r.headers["Cache-Control"] = "no-store"
    return r


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.get("/")
def home():
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))
    return redirect(url_for("ces"))


@app.get("/login")
def login():
    csrf = _get_csrf_token()
    _btn_style = "display:inline-block;padding:8px 16px;border:1px solid #666;border-radius:4px;text-decoration:none;font-size:0.95em;"
    _alt_btns = []
    if _ms_config()["client_id"]:
        _alt_btns.append(f'<a href="{url_for("login_microsoft")}" class="btn-secondary" style="{_btn_style}">Login com conta Microsoft UP</a>')
    if _oidc_config()["client_id"]:
        _alt_btns.append(f'<a href="{url_for("login_oidc")}" class="btn-secondary" style="{_btn_style}">Autenticação federada UP</a>')
    _alt_logins_html = (
        '<hr style="margin:18px 0;">'
        '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:4px;">' + "".join(_alt_btns) + "</div>"
    ) if _alt_btns else ""
    body = f"""
    <div class="card">
      <form id="login-form" method="post" action="{url_for('login_post')}">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <div class="row" style="align-items:center; gap:10px; max-width:400px;">
          <label style="width:78px; min-width:78px;">Utilizador:</label>
          <input name="login" autocomplete="username" placeholder="email institucional SIGARRA" required style="width:220px;">
        </div>
        <div class="row" style="margin-top:10px; align-items:center; gap:10px; max-width:400px;">
          <label style="width:78px; min-width:78px;">Senha:</label>
          <input name="password" type="password" autocomplete="current-password" style="width:220px; max-width:100%;" required>
        </div>
        <div class="row" style="margin-top:14px;">
          <button id="btn-login" type="submit">Autenticar</button>
        </div>
      </form>
      {_alt_logins_html}
      {'<p style="margin:14px 0 0;font-size:0.9em;">Ou <a href="' + url_for("login_email") + '">Entrar com código enviado por email</a></p>' if _resend_api_key() else ''}
      <p class="muted" style="margin-top:10px;"><a href="{url_for('privacidade')}">Política de privacidade e proteção de dados</a></p>
    </div>
    """
    return _page("Login no SIGARRA", body)


@app.post("/login")
@_limiter.limit("10 per minute; 30 per hour")
def login_post():
    _require_csrf()
    login_val = request.form.get("login", "").strip()
    # A API mob do SIGARRA aceita apenas o prefixo (ex: "jpf" ou "up202206705")
    if "@" in login_val:
        login_val = login_val.split("@")[0]
    password = request.form.get("password", "")

    sess = SigarraSession()
    try:
        sess.autenticar(login=login_val, password=password)
    except Exception as e:
        return _page("Login no SIGARRA", f"""
        <div class="card">
          <p><b>Falha na autenticação:</b> {_esc(e)}</p>
          <p><a href="{url_for('login')}">Voltar</a></p>
        </div>
        """)

    _set_sigarra_session(sess)
    flask_session["sigarra_login"] = login_val
    flask_session["login_method"] = "password"
    return redirect(url_for("ces"))


@app.get("/privacidade")
def privacidade():
    body = f"""
    <div class="card">
      <h3>Política de privacidade e proteção de dados</h3>
      <p class="muted">
        Esta aplicação encontra-se em fase piloto de teste e validação institucional.
        Os pareceres gerados com apoio de modelos de inteligência artificial podem conter
        imprecisões e devem ser sempre revistos antes da sua utilização.
      </p>

      <h4>Autenticação e comunicação segura</h4>
      <p>
        A aplicação suporta dois métodos de autenticação, ambos protegidos por HTTPS/TLS:
      </p>
      <ul>
        <li>
          <b>Login SIGARRA (utilizador e senha):</b> as credenciais são transmitidas pelo servidor
          desta aplicação para a API do SIGARRA para estabelecer uma sessão autenticada.
          As credenciais não são guardadas em disco nem registadas em logs.
        </li>
        <li>
          <b>Acesso por email institucional (upNNNNNN@up.pt ou upNNNNNN@edu.fe.up.pt):</b> o utilizador recebe um código
          temporário de uso único por email (válido 10 minutos), enviado através do serviço
          <a href="https://resend.com" target="_blank" rel="noopener">Resend</a>.
          O endereço de email não é guardado em disco após a verificação do código.
          A sessão SIGARRA é estabelecida através de uma conta de servidor com acesso alargado
          à consulta de relatórios de ciclos de estudos, sendo o código do utilizador registado
          nos metadados de auditoria.
        </li>
      </ul>

      <h4>Dados acedidos pela aplicação</h4>
      <p>
        A sessão autenticada é usada para aceder ao relatório do ciclo de estudo (CE) selecionado (versão para impressão), 
        o qual é enviado ao LLM para análise e geração de proposta de parecer. 
        Os dados estatísticos e textuais do relatório são os únicos dados enviados ao LLM.
        Não são processados dados pessoais individuais de estudantes.
      </p>

            <h4>Utilização de modelos de linguagem (LLM)</h4>
      <p>
        A aplicação utiliza modelos de linguagem de grande escala (LLM) para analisar o relatório do CE e elaborar proposta de parecer.
        As garantias de privacidade e proteção de dados aplicáveis dependem do fornecedor selecionado:
      </p>
      <ul>
        <li>
        <b>Via IAedu:</b> o processamento é efetuado através da infraestrutura Microsoft Azure AI Foundry disponibilizada
        pelo serviço IAedu da FCT/FCCN (sem custos diretos para a unidade orgânica utilizadora), limitado aos modelos aí
        disponibilizados. De acordo com a respetiva <a href="https://iaedu.pt/pt/politica-de-privacidade-e-protecao-de-dados" target="_blank" rel="noopener noreferrer">política de privacidade</a>, 
        os dados não são armazenados, registados, transmitidos a terceiros, utilizados para treino de modelos ou conservados sob qualquer forma.
        </li>
<li>
  <b>Via Anthropic API:</b> o processamento é efetuado através da API comercial da Anthropic.
  De acordo com a <a href="https://privacy.claude.com/en/collections/10672411-data-handling-retention">informação pública atualmente disponibilizada</a>, os dados enviados não são utilizados para treino de modelos,  podendo ser objeto de retenção temporária 
  (limitada por defeito a 30 dias) para fins de monitorização de segurança e prevenção de abuso.
          Quando aplicável, os custos de utilização são suportados institucionalmente pela FEUP,
        podendo ser definidos limites de utilização por utilizador no âmbito de políticas de utilização responsável.

</li>
      </ul>

      <h4>Registos técnicos e auditoria</h4>
      <p>
        Para fins de auditoria técnica, monitorização operacional e controlo de custos de utilização dos serviços LLM,
        são mantidos registos técnicos persistentes contendo apenas metadados de execução, incluindo o código do
        utilizador, o código do ciclo de estudos, data e hora da execução, identificador técnico da
        operação, modelo utilizado e custo estimado. Não são armazenados conteúdos processados nem credenciais de autenticação.
        Estes registos são utilizados exclusivamente para fins operacionais, de auditoria e gestão de custos.
      </p>

      <h4>Retenção e exportação de dados</h4>
      <p>
        Os dados gerados durante a execução podem ser exportados pelo utilizador em formato <code>.zip</code>.
        Estes dados são removidos automaticamente do disco após um período máximo de
        {WEB_OUTPUT_RETENTION_HOURS:.3g} hora(s) de retenção configurado no servidor.
      </p>

      <p class="muted">
        O código-fonte desta ferramenta é público e auditável em
        <a href="https://github.com/joaopascoalfariafeup/cereports" target="_blank" rel="noopener">github.com/joaopascoalfariafeup/cereports</a>.
      </p>

    </div>
    """
    return _page("Política de privacidade", body)


# ---------------------------------------------------------------------------
# Federated auth (proxy SAML — same as UC app)
# ---------------------------------------------------------------------------

_SAML_ASSET_PREFIX = "/login/federado/proxy"
_SAML_ASSET_BASE = "https://wayf.up.pt"


def _proxy_saml_html(html_doc: str, relay_url: str, token: str) -> str:
    pfx = _SAML_ASSET_PREFIX
    html_doc = re.sub(r'<meta[^>]+http-equiv=["\']?Content-Security-Policy["\']?[^>]*/?>', '', html_doc, flags=re.IGNORECASE)
    html_doc = re.sub(r'((?:href|src)=")(/[^"]*)', rf'\1{pfx}\2', html_doc, flags=re.IGNORECASE)
    html_doc = re.sub(r"((?:href|src)=')(/[^']*)", rf"\1{pfx}\2", html_doc, flags=re.IGNORECASE)
    html_doc = re.sub(r'(url\(["\']?)(/[^"\')\s]*)', rf'\1{pfx}\2', html_doc)
    base_tag = f'<base href="{_SAML_ASSET_BASE}/">'
    html_doc = re.sub(r'(<head[^>]*>)', r'\1' + base_tag, html_doc, count=1, flags=re.IGNORECASE)
    if '<base ' not in html_doc:
        html_doc = base_tag + html_doc
    html_doc = re.sub(
        r'(<form\b[^>]*\baction=)["\'][^"\']*["\']',
        rf'\1"{relay_url}"',
        html_doc, count=1, flags=re.IGNORECASE,
    )
    html_doc = re.sub(
        r'(<button\b[^>]*\bname=["\']_eventId_authn/[^"\']+["\'])([^>]*>)',
        r'\1 disabled title="Não disponível nesta interface"\2',
        html_doc, flags=re.IGNORECASE,
    )
    for _cb_name in ("_shib_idp_revokeConsent", "donotcache"):
        html_doc = re.sub(
            rf'(<input\b[^>]*\bname=["\']{re.escape(_cb_name)}["\'][^>]*)',
            r'\1 disabled title="Não disponível nesta interface"',
            html_doc, flags=re.IGNORECASE,
        )
    html_doc = html_doc.replace('</form>', f'<input type="hidden" name="_fed_token" value="{token}"></form>', 1)
    return html_doc


@app.get("/login/federado/proxy/<path:asset_path>")
def federado_asset_proxy(asset_path: str):
    url = f"{_SAML_ASSET_BASE}/{asset_path}"
    try:
        req = _urllib_req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = _urllib_req.urlopen(req, timeout=10)
        content = resp.read()
        content_type = resp.headers.get("Content-Type", "application/octet-stream")
        if "text/css" in content_type:
            charset = resp.headers.get_content_charset() or "utf-8"
            css = content.decode(charset, errors="replace")
            css = re.sub(r'(url\(["\']?)(/[^"\')\s]*)', rf'\1{_SAML_ASSET_PREFIX}\2', css)
            content = css.encode("utf-8")
            content_type = "text/css; charset=utf-8"
        r = Response(content, content_type=content_type)
        r.headers["Cache-Control"] = "public, max-age=3600"
        return r
    except Exception:
        abort(404)


@app.get("/login/federado")
def login_federado():
    sess = SigarraSession()
    try:
        html_e1s2, url_e1s2 = sess.autenticar_federado_iniciar()
    except Exception as e:
        return _page("Autenticação Federada UP", f"""
        <div class="card">
          <p><b>Falha ao iniciar autenticação federada:</b> {_esc(str(e))}</p>
          <p><a href="{url_for('login')}">Voltar ao login</a></p>
        </div>
        """)
    token = secrets.token_urlsafe(16)
    form_action = SigarraSession._saml_form_action(html_e1s2, url_e1s2)
    with _FED_STATES_LOCK:
        _FED_STATES[token] = (sess, form_action, "")
    relay_url = url_for("login_federado_relay", _external=True)
    proxied = _proxy_saml_html(html_e1s2, relay_url, token)
    return Response(proxied, content_type="text/html; charset=utf-8")


@app.post("/login/federado")
def login_federado_relay():
    token = request.form.get("_fed_token", "").strip()
    with _FED_STATES_LOCK:
        state = _FED_STATES.get(token)
    if not state:
        return _page("Autenticação Federada UP", f"""
        <div class="card">
          <p><b>Sessão de autenticação inválida ou expirada.</b></p>
          <p><a href="{url_for('login_federado')}">Recomeçar</a></p>
        </div>
        """)
    sess, forward_url, saved_username = state
    form_data = {k: v for k, v in request.form.items() if k != "_fed_token"}
    username_hint = form_data.get("j_username", "").strip() or saved_username

    try:
        html_next, url_next = sess._saml_request(forward_url, post_data=form_data, referer=forward_url)
    except Exception as e:
        with _FED_STATES_LOCK:
            _FED_STATES.pop(token, None)
        return _page("Autenticação Federada UP", f"""
        <div class="card">
          <p><b>Falha na comunicação com o IdP:</b> {_esc(str(e))}</p>
          <p><a href="{url_for('login_federado')}">Recomeçar</a></p>
        </div>
        """)

    # Resolver sondas localStorage server-side
    ls_iter = 0
    while "shib_idp_ls_success" in html_next and ls_iter < 5:
        ls_iter += 1
        if SigarraSession._saml_input_val(html_next, "SAMLResponse"):
            break
        ls_action = SigarraSession._saml_form_action(html_next, url_next)
        ls_data = {
            "csrf_token": SigarraSession._saml_input_val(html_next, "csrf_token"),
            "shib_idp_ls_exception.shib_idp_session_ss": "",
            "shib_idp_ls_success.shib_idp_session_ss": "true",
            "shib_idp_ls_value.shib_idp_session_ss": "",
            "shib_idp_ls_exception.shib_idp_persistent_ss": "",
            "shib_idp_ls_success.shib_idp_persistent_ss": "true",
            "shib_idp_ls_value.shib_idp_persistent_ss": "",
            "shib_idp_ls_supported": "true",
            "_eventId_proceed": "",
        }
        try:
            html_next, url_next = sess._saml_request(ls_action, post_data=ls_data, referer=url_next)
        except Exception as e:
            with _FED_STATES_LOCK:
                _FED_STATES.pop(token, None)
            return _page("Autenticação Federada UP", f"""
            <div class="card">
              <p><b>Falha na sonda de sessão do IdP:</b> {_esc(str(e))}</p>
              <p><a href="{url_for('login_federado')}">Recomeçar</a></p>
            </div>
            """)

    if SigarraSession._saml_input_val(html_next, "SAMLResponse"):
        try:
            sess.autenticar_federado_completar(html_next, url_next, username=username_hint)
        except Exception as e:
            with _FED_STATES_LOCK:
                _FED_STATES.pop(token, None)
            return _page("Autenticação Federada UP", f"""
            <div class="card">
              <p><b>Falha na autenticação:</b> {_esc(str(e))}</p>
              <p><a href="{url_for('login_federado')}">Recomeçar</a></p>
            </div>
            """)
        if not sess.codigo_pessoal:
            with _FED_STATES_LOCK:
                _FED_STATES.pop(token, None)
            return _page("Autenticação Federada UP", f"""
            <div class="card">
              <p><b>Sessão SIGARRA inválida após autenticação.</b></p>
              <p>Faça logout completo do SIGARRA no browser e tente novamente.</p>
              <p><a href="{url_for('login_federado')}">Recomeçar</a></p>
            </div>
            """)
        with _FED_STATES_LOCK:
            _FED_STATES.pop(token, None)
        _set_sigarra_session(sess)
        flask_session["sigarra_login"] = username_hint
        flask_session["login_method"] = "password"
        return redirect(url_for("ces"))

    # Ainda no fluxo (ex: MFA, etc.)
    with _FED_STATES_LOCK:
        _FED_STATES[token] = (sess, SigarraSession._saml_form_action(html_next, url_next), username_hint)
    relay_url = url_for("login_federado_relay", _external=True)
    proxied = _proxy_saml_html(html_next, relay_url, token)
    return Response(proxied, content_type="text/html; charset=utf-8")


@app.get("/logout")
def logout():
    _clear_sigarra_session()
    flask_session.pop("sigarra_login", None)
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Autenticação Microsoft OAuth (opcional; ativo se MS_CLIENT_ID configurado)
# ---------------------------------------------------------------------------

@app.get("/login/microsoft")
def login_microsoft():
    cfg = _ms_config()
    if not cfg["client_id"]:
        return _page("Erro", """<div class="card"><p>Autenticação Microsoft não configurada.</p></div>""")

    with _MS_STATES_LOCK:
        now = time.time()
        for k in [k for k, v in _MS_STATES.items() if v < now]:
            del _MS_STATES[k]
        state = secrets.token_urlsafe(24)
        _MS_STATES[state] = now + 300

    params = urllib.parse.urlencode({
        "client_id":     cfg["client_id"],
        "response_type": "code",
        "redirect_uri":  cfg["redirect_uri"],
        "scope":         "openid email profile",
        "state":         state,
        "response_mode": "query",
    })
    auth_url = f"https://login.microsoftonline.com/{cfg['tenant']}/oauth2/v2.0/authorize"
    return redirect(f"{auth_url}?{params}", code=302)


@app.get("/login/microsoft/callback")
def login_microsoft_callback():
    cfg = _ms_config()

    error = request.args.get("error")
    if error:
        desc = request.args.get("error_description", "")
        return _page("Login Microsoft", f"""
        <div class="card">
          <p><b>Erro na autenticação Microsoft:</b> {_esc(desc or error)}</p>
          <p><a href="{url_for('login')}">Voltar ao login</a></p>
        </div>""")

    code  = request.args.get("code",  "").strip()
    state = request.args.get("state", "").strip()

    with _MS_STATES_LOCK:
        if not state or _MS_STATES.pop(state, 0) < time.time():
            return _page("Login Microsoft", f"""
            <div class="card">
              <p><b>Sessão expirada ou inválida.</b></p>
              <p><a href="{url_for('login_microsoft')}">Tentar novamente</a></p>
            </div>""")

    # Trocar code por token junto do Microsoft
    try:
        payload = urllib.parse.urlencode({
            "grant_type":    "authorization_code",
            "code":           code,
            "redirect_uri":   cfg["redirect_uri"],
            "client_id":      cfg["client_id"],
            "client_secret":  cfg["client_secret"],
            "scope":          "openid email profile",
        }).encode()
        req = _urllib_req.Request(
            f"https://login.microsoftonline.com/{cfg['tenant']}/oauth2/v2.0/token",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with _urllib_req.urlopen(req, timeout=15) as resp:
            token_data = json.loads(resp.read().decode())
    except Exception as e:
        app.logger.warning("login_microsoft_callback: erro ao trocar token: %s", e)
        return _page("Login Microsoft", f"""
        <div class="card">
          <p><b>Erro ao contactar Microsoft:</b> {_esc(str(e))}</p>
          <p><a href="{url_for('login_microsoft')}">Tentar novamente</a></p>
        </div>""")

    # Extrair email do id_token (JWT; confiar no HTTPS + state CSRF para validação)
    email = ""
    id_token = token_data.get("id_token", "")
    if id_token:
        try:
            parts = id_token.split(".")
            if len(parts) >= 2:
                padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
                claims = json.loads(base64.urlsafe_b64decode(padded))
                email = claims.get("email") or claims.get("preferred_username") or ""
        except Exception:
            pass

    if not email:
        return _page("Login Microsoft", f"""
        <div class="card">
          <p><b>Não foi possível obter email da conta Microsoft.</b></p>
          <p><a href="{url_for('login')}">Usar login SIGARRA</a></p>
        </div>""")

    codigo = _codigo_de_email_ms(email)
    if not codigo:
        return _page("Login Microsoft", f"""
        <div class="card">
          <p><b>Email não reconhecido como conta UP:</b> {_esc(email)}</p>
          <p>É necessário o formato <code>up<i>número</i>@up.pt</code>.</p>
          <p><a href="{url_for('login')}">Usar login SIGARRA</a></p>
        </div>""")

    try:
        server_sess = _get_server_session()
    except Exception as e:
        app.logger.warning("login_microsoft_callback: sessão servidor indisponível: %s", e)
        return _page("Login Microsoft", f"""
        <div class="card">
          <p><b>Serviço temporariamente indisponível.</b> Tente mais tarde.</p>
          <p><a href="{url_for('login')}">Usar login SIGARRA</a></p>
        </div>""")

    user_sess = server_sess.clone_para_utilizador(codigo)
    _set_sigarra_session(user_sess)
    flask_session["sigarra_login"] = email
    flask_session["login_method"] = "microsoft"
    return redirect(url_for("ces"))


# ---------------------------------------------------------------------------
# Autenticação OIDC Keycloak UP (opcional; ativo se OIDC_CLIENT_ID configurado)
# ---------------------------------------------------------------------------

@app.get("/login/oidc")
def login_oidc():
    cfg = _oidc_config()
    if not cfg["client_id"]:
        return _page("Erro", """<div class="card"><p>Autenticação federada não configurada.</p></div>""")

    with _OIDC_STATES_LOCK:
        now = time.time()
        for k in [k for k, v in _OIDC_STATES.items() if v < now]:
            del _OIDC_STATES[k]
        state = secrets.token_urlsafe(24)
        _OIDC_STATES[state] = now + 300

    params = urllib.parse.urlencode({
        "client_id":     cfg["client_id"],
        "response_type": "code",
        "redirect_uri":  cfg["redirect_uri"],
        "scope":         "openid email profile",
        "state":         state,
        "response_mode": "query",
        "kc_idp_hint":   "saml",
    })
    resp = make_response(redirect(f"{cfg['auth_endpoint']}?{params}", code=302))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.get("/login/oidc/callback")
def login_oidc_callback():
    cfg = _oidc_config()

    error = request.args.get("error")
    if error:
        desc = request.args.get("error_description", "")
        return _page("Autenticação federada", f"""
        <div class="card">
          <p><b>Erro na autenticação:</b> {_esc(desc or error)}</p>
          <p><a href="{url_for('login')}">Voltar ao login</a></p>
        </div>""")

    code  = request.args.get("code",  "").strip()
    state = request.args.get("state", "").strip()

    with _OIDC_STATES_LOCK:
        if not state or _OIDC_STATES.pop(state, 0) < time.time():
            return _page("Autenticação federada", f"""
            <div class="card">
              <p><b>Sessão expirada ou inválida.</b></p>
              <p><a href="{url_for('login_oidc')}">Tentar novamente</a></p>
            </div>""")

    # Trocar code por token junto do Keycloak UP
    try:
        payload = urllib.parse.urlencode({
            "grant_type":   "authorization_code",
            "code":          code,
            "redirect_uri":  cfg["redirect_uri"],
            "client_id":     cfg["client_id"],
            "client_secret": cfg["client_secret"],
        }).encode()
        req = _urllib_req.Request(
            cfg["token_endpoint"],
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with _urllib_req.urlopen(req, timeout=15) as resp:
            token_data = json.loads(resp.read().decode())
    except Exception as e:
        app.logger.warning("login_oidc_callback: erro ao trocar token: %s", e)
        return _page("Autenticação federada", f"""
        <div class="card">
          <p><b>Erro ao contactar servidor de autenticação:</b> {_esc(str(e))}</p>
          <p><a href="{url_for('login_oidc')}">Tentar novamente</a></p>
        </div>""")

    # Extrair preferred_username do id_token (JWT)
    username = ""
    id_token = token_data.get("id_token", "")
    if id_token:
        try:
            parts = id_token.split(".")
            if len(parts) >= 2:
                padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
                claims = json.loads(base64.urlsafe_b64decode(padded))
                username = claims.get("preferred_username") or claims.get("sub") or ""
        except Exception:
            pass

    # preferred_username pode ser "up210006@up.pt" ou só "up210006"
    if "@" in username:
        username = username.split("@")[0]

    # Extrair código numérico (upNNNNNN → NNNNNN) ou aceitar código alfabético (jpf)
    if username.lower().startswith("up"):
        codigo = username[2:]
    else:
        codigo = username

    if not codigo:
        return _page("Autenticação federada", f"""
        <div class="card">
          <p><b>Não foi possível identificar o utilizador UP.</b></p>
          <p><a href="{url_for('login')}">Usar login SIGARRA</a></p>
        </div>""")

    # Tentar obter sessão SIGARRA real via troca de token OIDC
    # Tenta access_token e id_token (o SIGARRA pode exigir um ou outro)
    user_sess = None
    flask_session.pop("oidc_sess_debug", None)
    _debug_msgs = []
    for _tok_key in ("access_token", "id_token"):
        _tok = token_data.get(_tok_key, "")
        if not _tok:
            _debug_msgs.append(f"{_tok_key}: ausente")
            continue
        try:
            user_sess = SigarraSession.from_oidc_token(_tok, codigo)
            app.logger.info("login_oidc_callback: sessão SIGARRA obtida via %s para %s", _tok_key, codigo)
            flask_session["oidc_sess_debug"] = f"ok via {_tok_key}"
            break
        except Exception as e:
            _err_msg = f"{_tok_key}: {e}"
            app.logger.warning("login_oidc_callback: %s", _err_msg)
            _debug_msgs.append(_err_msg)
    if user_sess is None:
        flask_session["oidc_sess_debug"] = " | ".join(_debug_msgs)

    # Fallback: clonar sessão do servidor
    if user_sess is None:
        try:
            server_sess = _get_server_session()
        except Exception as e:
            app.logger.warning("login_oidc_callback: sessão servidor indisponível: %s", e)
            return _page("Autenticação federada", f"""
            <div class="card">
              <p><b>Serviço temporariamente indisponível.</b> Tente mais tarde.</p>
              <p><a href="{url_for('login')}">Usar login SIGARRA</a></p>
            </div>""")
        user_sess = server_sess.clone_para_utilizador(codigo)
        flask_session["oidc_sess_type"] = "clone"
    else:
        flask_session["oidc_sess_type"] = "direct"

    _set_sigarra_session(user_sess)
    flask_session["sigarra_login"] = username + "@up.pt"
    flask_session["login_method"] = "oidc"
    return redirect(url_for("ces"))


# ---------------------------------------------------------------------------
# Autenticação por email OTP (via Resend; ativo se RESEND_API_KEY configurado)
# ---------------------------------------------------------------------------

def _resend_api_key() -> str:
    return (os.environ.get("RESEND_API_KEY") or "").strip()


def _encaminhamento_ativo() -> bool:
    return bool(_resend_api_key()) and os.environ.get("ENABLE_REVIEW_FORWARDING", "0").strip() == "1"



def _resend_from() -> str:
    return (os.environ.get("RESEND_FROM") or "noreply@ce.uc-reports.com").strip()


def _codigo_de_email_otp(email: str) -> Optional[str]:
    """Extrai código SIGARRA de emails UP: upNNNNNN@up.pt ou upNNNNNN@*.up.pt."""
    m = re.match(r"^up(\d{6,9})@(?:[\w-]+\.)*up\.pt$", email.strip().lower())
    return m.group(1) if m else None


def _purge_expired_otps() -> None:
    now = time.time()
    with _OTPS_LOCK:
        expired = [k for k, v in _OTPS.items() if v["expires"] < now]
        for k in expired:
            del _OTPS[k]


def _send_otp_email(to_email: str, otp: str) -> None:
    """Envia OTP via Resend API."""
    payload = json.dumps({
        "from":    _resend_from(),
        "to":      [to_email],
        "subject": "Código de acesso — Pareceres CE FEUP",
        "html":    (
            f"<p>O seu código de acesso temporário é:</p>"
            f"<p style='font-size:2em;letter-spacing:0.2em;font-weight:bold'>{otp}</p>"
            f"<p>Válido por 10 minutos. Não partilhe este código.</p>"
        ),
    }).encode()
    req = _urllib_req.Request(
        "https://api.resend.com/emails",
        data=payload,
        headers={
            "Authorization": f"Bearer {_resend_api_key()}",
            "Content-Type":  "application/json",
            "User-Agent":    "CEReports/1.0",
        },
        method="POST",
    )
    try:
        with _urllib_req.urlopen(req, timeout=15) as resp:
            if resp.status not in (200, 201):
                body = resp.read().decode("utf-8", errors="replace")[:300]
                raise RuntimeError(f"Resend HTTP {resp.status}: {body}")
    except _urllib_req.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"Resend HTTP {e.code}: {body}") from e


@app.get("/login/email")
def login_email():
    if not _resend_api_key():
        abort(404)
    csrf = _get_csrf_token()
    return _page("Acesso por email institucional", f"""
    <div class="card">
      <p>Introduza o seu email UP (ex: upNNNNNN@up.pt ou upNNNNNN@edu.fe.up.pt).</p>
      <form method="post" action="{url_for('login_email_post')}">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <div class="row" style="align-items:center; gap:10px; max-width:400px;">
          <label style="width:60px; min-width:60px;">Email:</label>
          <input name="email" type="email" autocomplete="email"
                 placeholder="upNNNNNN@up.pt" required style="width:240px;">
        </div>
        <div class="row" style="margin-top:14px;">
          <button type="submit">Enviar código</button>
        </div>
      </form>
      <p style="margin-top:16px;"><a href="{url_for('login')}">&#8592; Voltar ao login SIGARRA</a></p>
    </div>""")


@app.post("/login/email")
@_limiter.limit("5 per minute; 20 per hour")
def login_email_post():
    if not _resend_api_key():
        abort(404)
    _require_csrf()
    email = (request.form.get("email") or "").strip().lower()
    codigo = _codigo_de_email_otp(email)
    if not codigo:
        return _page("Acesso por email", f"""
        <div class="card">
          <p><b>Email não reconhecido.</b> Use um email UP no formato upNNNNNN@up.pt ou upNNNNNN@edu.fe.up.pt.</p>
          <p><a href="{url_for('login_email')}">Tentar novamente</a></p>
        </div>""")

    otp = "{:06d}".format(secrets.randbelow(1_000_000))
    _purge_expired_otps()
    with _OTPS_LOCK:
        _OTPS[email] = {"otp": otp, "codigo": codigo, "expires": time.time() + 600}

    try:
        _send_otp_email(email, otp)
    except Exception as e:
        app.logger.warning("login_email_post: erro ao enviar OTP para %s: %s", email, e)
        return _page("Acesso por email", f"""
        <div class="card">
          <p><b>Erro ao enviar email:</b></p>
          <p><code style="font-size:0.85em;word-break:break-all;">{_esc(str(e))}</code></p>
          <p><a href="{url_for('login_email')}">Tentar novamente</a> &nbsp;|&nbsp; <a href="{url_for('login')}">Login SIGARRA</a></p>
        </div>""")

    csrf = _get_csrf_token()
    return _page("Acesso por email institucional", f"""
    <div class="card">
      <p>Enviámos um código de 6 dígitos para {_esc(email)}.<br>
         Válido por 10 minutos.</p>
      <form method="post" action="{url_for('login_email_verificar')}">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <input type="hidden" name="email" value="{_esc(email)}">
        <div class="row" style="align-items:center; gap:10px; max-width:360px;">
          <label style="white-space:nowrap;">Introduza o código recebido:</label>
          <input name="otp" type="text" inputmode="numeric" pattern="[0-9]{{6}}"
                 maxlength="6" autocomplete="one-time-code"
                 placeholder="000000" required style="width:120px; font-size:1.3em; letter-spacing:0.15em;">
        </div>
        <div class="row" style="margin-top:14px;">
          <button type="submit">Verificar</button>
        </div>
      </form>
      <p style="margin-top:16px;"><a href="{url_for('login_email')}">Pedir novo código</a></p>
    </div>""")


@app.post("/login/email/verificar")
@_limiter.limit("10 per minute")
def login_email_verificar():
    if not _resend_api_key():
        abort(404)
    _require_csrf()
    email = (request.form.get("email") or "").strip().lower()
    otp   = (request.form.get("otp")   or "").strip()

    _purge_expired_otps()
    with _OTPS_LOCK:
        entry = _OTPS.get(email)
        if not entry or entry["otp"] != otp or entry["expires"] < time.time():
            return _page("Acesso por email", f"""
            <div class="card">
              <p><b>Código inválido ou expirado.</b></p>
              <p><a href="{url_for('login_email')}">Pedir novo código</a></p>
            </div>""")
        codigo = entry["codigo"]
        del _OTPS[email]   # single-use

    try:
        server_sess = _get_server_session()
        user_sess = server_sess.clone_para_utilizador(codigo)
    except Exception as e:
        app.logger.warning("login_email_verificar: erro ao criar sessão para %s: %s", email, e)
        return _page("Acesso por email", f"""
        <div class="card">
          <p><b>Erro ao criar sessão:</b></p>
          <p><code style="font-size:0.85em;word-break:break-all;">{_esc(str(e))}</code></p>
          <p><a href="{url_for('login')}">Login SIGARRA</a></p>
        </div>""")

    _set_sigarra_session(user_sess)
    flask_session["sigarra_login"] = email
    flask_session["login_method"] = "otp"
    return redirect(url_for("ces"))


# ---------------------------------------------------------------------------
# API: relatórios disponíveis para um CE (chamado via fetch do browser)
# ---------------------------------------------------------------------------

@app.get("/api/relatorios_ce/<cur_id>")
def api_relatorios_ce(cur_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return Response('{"error":"unauthorized"}', status=401, mimetype="application/json")
    if not re.match(r'^\d+$', cur_id):
        return Response('{"error":"invalid"}', status=400, mimetype="application/json")

    relatorios = listar_relatorios_ce(cur_id, sessao=sess)
    anos = []
    for r in relatorios:
        ano = r["ano"]
        y = int(ano)
        label = f"{y}/{(y + 1) % 100:02d}"
        anos.append({"value": ano, "label": label, "pv_id": r["pv_id"]})

    if not anos:
        app.logger.warning("api_relatorios_ce cur_id=%s user=%s: lista vazia", cur_id, sess.codigo_pessoal)
    return Response(json.dumps({"anos": anos}), mimetype="application/json")


# ---------------------------------------------------------------------------
# Seleção de CE
# ---------------------------------------------------------------------------

@app.post("/impersonate")
def impersonate():
    _require_csrf()
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))
    if not _is_admin(sess):
        abort(403)
    code = request.form.get("impersonate_code", "").strip()
    if code:
        flask_session["impersonated_code"] = code
    else:
        flask_session.pop("impersonated_code", None)
    return redirect(url_for("ces"))


@app.get("/ces")
def ces():
    sess = _get_sigarra_session()
    if not sess:
        return _page("Sessão expirada", f"""
        <div class="card">
          <p>A sua sessão local expirou. Faça login novamente.</p>
          <p><a href="{url_for('login')}">Ir para login</a></p>
        </div>
        """)

    csrf = _get_csrf_token()
    provider_opts = _llm_provider_options()
    model_map = _llm_model_options_map()
    provider_default = (os.environ.get("LLM_PROVIDER", "anthropic") or "anthropic").strip().lower()

    llm_choices: list[dict[str, str]] = []
    for p in provider_opts:
        modelos = model_map.get(p) or [_default_modelo_por_provider(p)]
        for m in modelos:
            val = f"{p}::{m}"
            label = f"{p} / {m}{_format_model_cost(p)}"
            llm_choices.append({"provider": p, "modelo": m, "value": val, "label": label})

    default_choice = (os.environ.get("WEB_LLM_DEFAULT_CHOICE", "") or "").strip()
    valid_choices = {c["value"] for c in llm_choices}
    if default_choice not in valid_choices:
        default_choice = llm_choices[0]["value"] if llm_choices else ""
    last_llm_choice = flask_session.get("last_llm_choice", "")
    if last_llm_choice in valid_choices:
        default_choice = last_llm_choice

    llm_choice_option_tags = "\n".join(
        f'<option value="{_esc(c["value"])}"'
        + (" selected" if c["value"] == default_choice else "")
        + f' data-provider="{_esc(c["provider"])}" data-modelo="{_esc(c["modelo"])}">{_esc(c["label"])}</option>'
        for c in llm_choices
    )

    anos_fallback = _gera_lista_anos_letivos()
    anos_fallback_json = _esc(json.dumps(anos_fallback))

    # --- Dropdown de CEs a partir da página pública do SIGARRA ---
    ces_list = listar_ces_publicos()

    # --- Cargos relevantes do utilizador (admin pode impersonar) ---
    is_admin = _is_admin(sess)
    impersonated = _get_impersonated_code()
    eff_code = _effective_codigo(sess)
    cargos = obter_cargos_docente(sess, eff_code)

    # Permissões por tipo e por cur_id
    _permit_tipos: set[str] = set()
    if cargos["is_cp"]:
        _permit_tipos.update({"L", "M"})
    if cargos["is_cc"]:
        _permit_tipos.update({"L", "M", "D"})
    _ca_ids = {c["cur_id"] for c in cargos["cac_cursos"]}
    _director_ids = {d["cur_id"] for d in cargos["director_cursos"]}
    _has_cargos = bool(_permit_tipos or _ca_ids or _director_ids)

    def _ce_permitido(ce: dict) -> tuple[bool, str]:
        if is_admin:
            return True, ""  # admin tem acesso a todos os CEs e perspetivas
        if ce["cur_id"] in _director_ids:
            return True, ""  # diretor pode solicitar auto-avaliação
        if not _has_cargos:
            return True, ""  # sem cargos identificados: não restringir
        if ce["tipo"] in _permit_tipos:
            return True, ""
        if ce["cur_id"] in _ca_ids and ce["tipo"] == "D":
            return True, ""  # CA só para doutoramentos
        return False, "Sem cargo que permita emitir parecer para este CE"

    nome_docente = cargos.get("nome", "") or eff_code
    _papel = "Estudante" if re.match(r"^20\d{7,}$", eff_code) else "Docente"
    docente_label = f'{_papel}: {_esc(nome_docente)} ({_esc(eff_code)})' if eff_code else ""

    cargos_items = []
    if cargos["is_cp"]:
        cargos_items.append("Conselho Pedagógico — pode emitir parecer CP de licenciaturas e mestrados")
    if cargos["is_cc"]:
        cargos_items.append("Conselho Científico — pode emitir parecer CC de licenciaturas, mestrados e doutoramentos")
    _ces_tipo_map = {c["cur_id"]: c["tipo"] for c in ces_list}
    for c in cargos["cac_cursos"]:
        label_curso = _esc(c["sigla"] or c["nome"])
        s, n = (c["sigla"] or "").upper(), (c["nome"] or "").lower()
        artigo = "da" if s.startswith("L.") or n.startswith("licenciatura") else "do"
        tipo_ce = _ces_tipo_map.get(c["cur_id"], "")
        if tipo_ce == "D":
            cargos_items.append(f'Comissão de Acompanhamento {artigo} {label_curso} — pode emitir parecer de CA')
        else:
            cargos_items.append(f'Comissão de Acompanhamento {artigo} {label_curso} — CA não emite parecer (só em doutoramentos)')
    for d in cargos["director_cursos"]:
        s_dir = (d["sigla"] or "").upper()
        n_dir = (d["nome"] or "").lower()
        artigo_dir = "da" if s_dir.startswith("L.") or n_dir.startswith("licenciatura") else "do"
        label_dir = _esc(d["sigla"] or d["nome"] or d["cur_id"])
        cargos_items.append(
            f'Diretor {artigo_dir} {label_dir} — pode solicitar auto-avaliação'
        )

    cargos_li_html = ""
    if cargos_items:
        cargos_li_html = "<ul style='margin:2px 0 0;padding-left:20px;'>" + "".join(f"<li>{i}</li>" for i in cargos_items) + "</ul>"

    if impersonated:
        impersonate_banner = f"""
        <form method="post" action="{url_for('impersonate')}" style="display:inline;">
          <input type="hidden" name="csrf_token" value="{_esc(_get_csrf_token())}">
          <input type="hidden" name="impersonate_code" value="">
          <button type="submit" class="btn-edit" style="margin-left:8px;">Sair do modo</button>
        </form>"""
        nome_impersonado = f'{_esc(nome_docente)} ({_esc(eff_code)})' if eff_code else _esc(eff_code)
        cargos_html = f"""<div class="status-err" style="margin:0 0 10px;padding:8px 12px;border-radius:6px;font-size:0.9em;">
          <strong>Assumindo o papel de:</strong> {nome_impersonado}{impersonate_banner}
          {cargos_li_html}
        </div>"""
    else:
        _oidc_sess_note = ""
        if flask_session.get("login_method") == "oidc" and is_admin:
            _oidc_sess_type = flask_session.get("oidc_sess_type", "")
            _oidc_debug = flask_session.get("oidc_sess_debug", "")
            if _oidc_sess_type == "direct":
                _oidc_sess_note = ' <span style="color:green;font-size:0.85em;">(sessão SIGARRA direta)</span>'
            elif _oidc_sess_type == "clone":
                _debug_detail = f' — {_esc(_oidc_debug)}' if _oidc_debug else ""
                _oidc_sess_note = f' <span style="color:orange;font-size:0.85em;">(sessão clonada do servidor{_debug_detail})</span>'
        cargos_html = f"""<div class="muted" style="margin:0 0 10px;font-size:0.9em;">
          {f'<strong>{docente_label}</strong>{_oidc_sess_note}' if docente_label else ''}
          {cargos_li_html if cargos_li_html else ('<p style="margin:2px 0 0;">Sem cargos relevantes identificados no SIGARRA.</p>' if eff_code else '')}
        </div>"""

    # --- Impersonação (apenas admin) ---
    impersonate_html = ""
    if is_admin:
        csrf = _get_csrf_token()
        impersonate_html = f"""
        <details style="margin:0 0 12px;font-size:0.9em;">
          <summary style="cursor:pointer;color:var(--muted);">&#9881; Admin — assumir papel de utilizador</summary>
          <form method="post" action="{url_for('impersonate')}" style="margin-top:8px;display:flex;gap:8px;align-items:center;">
            <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
            <label for="impersonate_code">Código SIGARRA:</label>
            <input type="text" name="impersonate_code" id="impersonate_code"
                   placeholder="ex: 210006" value="{_esc(impersonated or '')}"
                   style="width:120px;" pattern="\\d+" title="Código numérico SIGARRA">
            <button type="submit" class="btn-edit">Assumir papel</button>
          </form>
        </details>"""
    last_ce_nome = flask_session.get("last_ce_nome", "")
    if ces_list:
        _TIPO_LABELS = {"L": "Licenciaturas", "M": "Mestrados", "D": "Doutoramentos"}
        optgroups = ""
        for tipo in ("L", "M", "D"):
            ces_tipo = [c for c in ces_list if c["tipo"] == tipo]
            if not ces_tipo:
                continue
            optgroups += f'<optgroup label="{_TIPO_LABELS[tipo]}">'
            for ce in ces_tipo:
                permitido, motivo = _ce_permitido(ce)
                disabled_attr = "" if permitido else f' disabled title="{_esc(motivo)}"'
                sel = " selected" if ce["nome"] == last_ce_nome and permitido else ""
                if is_admin and not impersonated:
                    persp_list = _perspetivas_disponiveis(ce, False, False, set(), set(), is_admin=True)
                elif _has_cargos:
                    persp_list = _perspetivas_disponiveis(
                        ce, cargos["is_cc"], cargos["is_cp"], _ca_ids, _director_ids
                    )
                else:
                    persp_list = []
                persp_json = _esc(json.dumps(persp_list, ensure_ascii=False))
                optgroups += (
                    f'<option value="{_esc(ce["nome"])}" data-cur-id="{_esc(ce["cur_id"])}"'
                    f' data-perspetivas="{persp_json}"{sel}{disabled_attr}>'
                    f'{_esc(ce["nome"])}</option>'
                )
            optgroups += "</optgroup>"
        # Determinar pré-seleção: último válido ou, se não existir, primeiro permitido
        ces_permitidos = [c for c in ces_list if _ce_permitido(c)[0]]
        if last_ce_nome and any(c["nome"] == last_ce_nome for c in ces_permitidos):
            default_ce = last_ce_nome
        elif ces_permitidos:
            default_ce = ces_permitidos[0]["nome"]
        else:
            default_ce = ""
        # Re-aplicar selected ao CE padrão (pode ter mudado para o 1º permitido)
        if default_ce and default_ce != last_ce_nome:
            optgroups = optgroups.replace(
                f'value="{_esc(default_ce)}"',
                f'value="{_esc(default_ce)}" selected',
                1,
            )
        ce_field_html = f"""
        <div class="form-row-inline">
          <label for="ce_nome">Ciclo de estudos:</label>
          <select name="ce_nome" id="ce_nome" required style="max-width:560px;">
            <option value="" disabled{'' if default_ce else ' selected'}>Selecione um ciclo de estudos...</option>
            {optgroups}
          </select>
        </div>"""
    else:
        # Fallback: input de texto se o SIGARRA não estiver acessível
        ce_field_html = f"""
        <div class="form-row-inline">
          <label for="ce_nome">Ciclo de estudos:</label>
          <input type="text" name="ce_nome" id="ce_nome"
                 placeholder="ex: Mestrado em Engenharia de Software"
                 value="{_esc(last_ce_nome)}" style="max-width:500px;" required>
        </div>"""

    # Se o utilizador tem cargos identificados mas nenhum CE é permitido, esconder formulário
    ces_permitidos_existem = not _has_cargos or bool(ces_permitidos if ces_list else True)
    if not ces_permitidos_existem:
        form_html = '<p class="muted" style="margin-top:8px;">Não tem cargos que permitam emitir parecer para nenhum dos ciclos de estudos disponíveis.</p>'
    else:
        form_html = f"""<form method="post" action="{url_for('start_job')}" enctype="multipart/form-data"
            style="margin-top:4px;">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <input type="hidden" name="cur_id" id="cur_id_hidden">

        {ce_field_html}

        <div class="form-row-inline">
          <label for="ano_letivo">Ano letivo:</label>
          <select name="ano_letivo" id="ano_letivo" style="max-width:100px;"
                  data-fallback-anos="{anos_fallback_json}">
            <option value="" disabled selected>—</option>
          </select>
        </div>

        <input type="hidden" name="pv_id" id="pv_id" value="">

        <div class="form-row-inline" id="perspetiva-row">
          <label for="perspetiva">Perspetiva:</label>
          <select name="perspetiva" id="perspetiva" style="max-width:240px;">
            <!-- populated by JS on CE change -->
          </select>
        </div>

        <div class="form-row-inline">
          <label for="llm_choice_select">Modelo:</label>
          <select name="llm_choice" id="llm_choice_select" style="max-width:240px;">
            {llm_choice_option_tags}
          </select>
        </div>
        <p class="muted" style="margin:6px 0 0 160px;font-size:0.88em;">Sugestão: use o modelo gratuito para testes e o claude-opus-4-6 para o parecer final.</p>

        <details style="margin-top:14px;"{' open' if flask_session.get('last_instrucoes') else ''}>
          <summary style="cursor:pointer; color:#555; font-size:0.93em;">Instruções adicionais (opcional)</summary>
          <div style="margin-top:8px;">
            <textarea name="instrucoes" id="instrucoes" rows="4"
              style="width:100%; max-width:600px; font-size:0.93em; resize:vertical;"
              maxlength="2000"
              placeholder="Preocupações específicas ou aspetos que pretende ver analisados..."
              >{_esc(flask_session.get('last_instrucoes', ''))}</textarea>
            <p class="muted" style="margin:2px 0 0; font-size:0.85em;">Máximo 2000 caracteres. Estas instruções serão incluídas no pedido ao LLM.</p>
          </div>
        </details>

        <div class="row" style="justify-content:flex-start; margin-top:14px;">
          <button class="btn" type="submit">Gerar parecer</button>
        </div>
      </form>"""

    # Banner de revisões pendentes
    eff_code_ces = _effective_codigo(sess)
    pending_reviews = _reviews_for_user(eff_code_ces) if eff_code_ces else []
    if pending_reviews:
        items = "".join(
            f'<li><a href="{url_for("revisao_get", token=r["token"])}">'
            f'{_esc(r["ce_nome"])} {_esc(r["ano_letivo"])} '
            f'({_esc(_PERSPETIVA_LABELS_WEB.get(r["perspetiva"], r["perspetiva"]))})'
            f'</a> — pedido por {_esc(r["owner_code"])}</li>'
            for r in pending_reviews
        )
        reviews_banner = f'<div class="card" style="border-left:4px solid #c00;"><p><b>Pareceres aguardando revisão sua:</b></p><ul style="margin:4px 0 0 20px;">{items}</ul></div>'
    else:
        reviews_banner = ""

    body = f"""
    {reviews_banner}
    <div class="card">
      {impersonate_html}
      {cargos_html}
      {form_html}
    </div>
    """
    return _page("Seleção do Ciclo de Estudos", body, step=1)


# ---------------------------------------------------------------------------
# Iniciar job
# ---------------------------------------------------------------------------

def _run_job(job: Tarefa, sess: SigarraSession, verbosidade: int) -> None:
    """Executa a análise e marca o job como concluído."""
    try:
        with AuditoriaLogger(job.log_path, verbosidade=verbosidade) as log:
            log.cabecalho(job.job_id, usuario=job.user_code)

            # Fase 1 — obter relatório do SIGARRA
            log.iniciar_fase("sigarra", "A obter relatório do SIGARRA...")
            try:
                relatorio_html = obter_relatorio_ce_html(job.pv_id, sess)
                log.concluir_fase(
                    "sigarra",
                    f"Relatório obtido ({len(relatorio_html) // 1024} KB)",
                )
            except Exception as e:
                log.concluir_fase("sigarra", f"Falha ao obter relatório: {e}", ok=False)
                raise

            # Fase 1b — pareceres do ano anterior (opcional, falha silenciosa)
            pareceres_anteriores = None
            if job.cur_id:
                try:
                    ano_raw = job.ano_letivo[:4]
                    pareceres_anteriores = obter_pareceres_ano_anterior(job.cur_id, ano_raw, sess)
                    if pareceres_anteriores:
                        log.info(f"  Pareceres do ano anterior obtidos ({len(pareceres_anteriores)} chars)")
                    else:
                        log.info("  Pareceres do ano anterior: não encontrados")
                except Exception as e:
                    log.info(f"  Pareceres do ano anterior: erro ({e})")

            # Fase 2 — análise por LLM (logada internamente por analisar_ce)
            analisar_ce(
                relatorio_html=relatorio_html,
                ce_nome=job.ce_nome,
                ano_letivo=job.ano_letivo,
                provider=job.llm_provider,
                modelo=job.llm_modelo,
                run_dir=job.run_dir,
                logger=log,
                pareceres_anteriores=pareceres_anteriores,
                perspetiva=job.perspetiva,
                instrucoes=job.instrucoes,
            )
        job.ok = True
    except Exception as e:
        job.ok = False
        job.error = str(e)
        try:
            job.log_path.parent.mkdir(parents=True, exist_ok=True)
            with job.log_path.open("a", encoding="utf-8", errors="replace") as f:
                f.write(f"\n# erro-sistema: {job.error}\n")
        except Exception:
            pass
    finally:
        if job.log_path.exists() and job.user_code:
            custo_job = _extrair_custo_estimado_valor(job.log_path)
            duracao_total_s = max(0.0, time.time() - float(job.started_at or 0.0))
            _add_user_cost_month(job.user_code, custo_job)
            _append_usage_event(
                job.user_code, job.ce_nome, custo_job,
                job.job_id, duracao_total_s,
                job.llm_provider, job.llm_modelo,
            )
        job.done = True


@app.post("/start")
def start_job():
    _require_csrf()
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    if _DRAINING_FILE.exists():
        return _page("Manutenção", f"""
        <div class="card">
          <p class="status-err"><b>Servidor em manutenção.</b> Tente novamente em breve.</p>
          <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar</a></p>
        </div>"""), 503

    ce_nome = request.form.get("ce_nome", "").strip()
    cur_id = request.form.get("cur_id", "").strip()
    ano_letivo = request.form.get("ano_letivo", "").strip()
    pv_id = request.form.get("pv_id", "").strip()
    llm_choice = request.form.get("llm_choice", "").strip()
    perspetiva = request.form.get("perspetiva", "").strip().upper()
    if perspetiva not in ("CC", "CP", "CA", "DCE"):
        perspetiva = ""

    if not ce_nome:
        return redirect(url_for("ces"))

    # Validação de permissões server-side
    eff_code = _effective_codigo(sess)
    if eff_code and cur_id and not _is_admin(sess):
        cargos = obter_cargos_docente(sess, eff_code)
        permit_tipos: set[str] = set()
        if cargos["is_cp"]:
            permit_tipos.update({"L", "M"})
        if cargos["is_cc"]:
            permit_tipos.update({"L", "M", "D"})
        ca_ids = {c["cur_id"] for c in cargos["cac_cursos"]}
        director_ids = {d["cur_id"] for d in cargos["director_cursos"]}
        has_cargos = bool(permit_tipos or ca_ids or director_ids)
        if has_cargos:
            ces_pub = listar_ces_publicos()
            ce_tipo = next((c["tipo"] for c in ces_pub if c["cur_id"] == cur_id), None)
            permitido = (
                cur_id in director_ids  # diretor pode solicitar auto-avaliação
                or ce_tipo in permit_tipos
                or (cur_id in ca_ids and ce_tipo == "D")  # CA só para doutoramentos
            )
            if not permitido:
                return _page("Sem permissão", f"""
                <div class="card">
                  <p class="status-err">Não tem permissão para emitir parecer para este ciclo de estudos.</p>
                  <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar</a></p>
                </div>"""), 403
            # Diretor deste CE só pode emitir auto-avaliação (DCE)
            if cur_id in director_ids and perspetiva in ("CC", "CP", "CA"):
                perspetiva = "DCE"

    if not pv_id or not re.match(r'^(3c:)?\d+$', pv_id):
        return _page("Erro", f"""
        <div class="card">
          <p class="status-err">Relatório não identificado. Selecione um ciclo de estudos e um ano letivo.</p>
          <p><a href="{url_for('ces')}">Voltar</a></p>
        </div>""")

    # Provider / modelo
    llm_provider = (os.environ.get("LLM_PROVIDER", "anthropic") or "anthropic").strip().lower()
    llm_modelo = _default_modelo_por_provider(llm_provider)
    if llm_choice and "::" in llm_choice:
        p, m = llm_choice.split("::", 1)
        llm_provider = p.strip().lower()
        llm_modelo = m.strip()

    providers_validos = _llm_provider_options()
    if llm_provider not in providers_validos and providers_validos:
        llm_provider = providers_validos[0]
        llm_modelo = _default_modelo_por_provider(llm_provider)

    instrucoes = request.form.get("instrucoes", "").strip()[:2000]

    flask_session["last_llm_choice"] = llm_choice or f"{llm_provider}::{llm_modelo}"
    flask_session["last_ce_nome"] = ce_nome
    flask_session["last_instrucoes"] = instrucoes

    # Verificar limite de custo mensal por utilizador
    user_code = (sess.codigo_pessoal or "").strip()
    max_usd_mes = _max_usd_per_user_per_month()
    if max_usd_mes > 0 and user_code and not _user_has_cost_bypass(user_code):
        usado = _user_cost_month(user_code)
        if usado >= max_usd_mes:
            if llm_provider not in WEB_FREE_LLM_PROVIDERS_SET:
                free_list = ", ".join(WEB_FREE_LLM_PROVIDERS_LIST) or "(nenhum)"
                return _page("Limite mensal atingido", f"""
                <div class="card">
                  <p class="status-err"><b>Limite mensal atingido:</b> ${usado:.2f} / ${max_usd_mes:.2f}</p>
                  <p class="muted">Com limite atingido, apenas são permitidos providers gratuitos: <code>{_esc(free_list)}</code>.</p>
                  <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar</a></p>
                </div>"""), 429

    with _JOBS_LOCK:
        em_execucao = sum(1 for j in _JOBS.values() if not j.done)
        if em_execucao >= MAX_RUNNING_JOBS:
            return _page("Servidor ocupado", f"""
            <div class="card">
              <p class="status-err">O servidor está a processar o número máximo de pedidos ({MAX_RUNNING_JOBS}).
              Por favor aguarde e tente novamente em breve.</p>
              <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar</a></p>
            </div>"""), 503

    _prune_output_dir()

    job_id = secrets.token_urlsafe(16)
    run_dir = OUTPUT_DIR / job_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "auditoria.log"

    job = Tarefa(
        job_id=job_id,
        log_path=log_path,
        started_at=time.time(),
        ce_nome=ce_nome,
        ano_letivo=_format_ano_letivo_display(ano_letivo),
        pv_id=pv_id,
        cur_id=cur_id,
        perspetiva=perspetiva,
        instrucoes=instrucoes,
        user_code=user_code,
        llm_provider=llm_provider,
        llm_modelo=llm_modelo,
        run_dir=run_dir,
    )

    with _JOBS_LOCK:
        _JOBS[job_id] = job

    t = threading.Thread(
        target=_run_job,
        args=(job, sess, WEB_VERBOSIDADE),
        daemon=True,
    )
    t.start()

    return redirect(url_for("progress", job_id=job_id))


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------

@app.get("/progress/<job_id>")
def progress(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return _page("Tarefa não encontrada", f"""
            <div class="card">
              <p>Tarefa não encontrada (talvez o processo tenha reiniciado).</p>
              <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar à seleção</a></p>
            </div>
            """)
        if not _is_job_owner(job, sess):
            return _page("Acesso negado", """
            <div class="card"><p class="status-err">Não tens permissões para este job.</p></div>
            """), 403

    if job.done and job.ok:
        estado = '<span class="status-ok">Parecer gerado com sucesso</span>'
    elif job.done and not job.ok:
        estado = f'<span class="status-err">Geração falhou: {_esc(job.error or "erro desconhecido")}</span>'
    else:
        estado = '<span class="status-run">A gerar parecer... pode demorar alguns minutos</span>'

    should_reload_on_done = "true" if not job.done else "false"
    ano_label = job.ano_letivo

    body = f"""
    <div class="card">
      {_ce_titulo_html(job.ce_nome, ano_label)}
      <div class="muted">{estado}</div>
    </div>
    """
    body += f"""
    <pre id="console" data-events-url="{_esc(url_for('events', job_id=job_id))}" data-should-reload-on-done="{should_reload_on_done}"></pre>
    """

    if job.done and job.ok:
        body += f"""
        <div class="card">
          <div class="navbar">
            <div class="navbar-left">
              <a class="btn" href="{url_for('preview', job_id=job_id)}">Rever parecer</a>
            </div>
            <div class="navbar-right">
              <a class="muted" href="{url_for('download_zip', job_id=job_id)}">Exportar (.zip)</a>
            </div>
          </div>
        </div>
        """
    elif job.done:
        body += f"""
        <div class="card">
          <p><a class="muted" href="{url_for('download_zip', job_id=job_id)}">Exportar dados (.zip)</a></p>
        </div>
        """

    return _page("Geração do Parecer", f'<div id="progress-body">{body}</div>', step=2)


@app.get("/events/<job_id>")
def events(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return Response("unauthorized", status=401)

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return Response("job not found", status=404)
        if not _is_job_owner(job, sess):
            return Response("forbidden", status=403)

    _last_event_id = request.headers.get("Last-Event-ID", "") or ""

    def generate():
        try:
            last_pos = int(_last_event_id)
        except (ValueError, TypeError):
            last_pos = 0

        def _filtrar_fases(txt: str) -> str:
            linhas = txt.splitlines(keepends=True)
            keep = []
            for ln in linhas:
                if any(tag in ln for tag in (
                    "--- Sumário ---", "Chamadas LLM:", "Modelo(s):",
                    "Tokens:", "Tempo LLM:", "Custo estimado:", "Tempo total:",
                )):
                    continue
                if "[FASE]" in ln or "[ERRO]" in ln or "[AVIS]" in ln:
                    ln = re.sub(r"^\[\d{2}:\d{2}:\d{2}\.\d{3}\]\s*", "", ln)
                    keep.append(
                        ln.replace("[FASE] ", "")
                        .replace("[AVIS] ", "")
                        .replace("[ERRO] ", "")
                    )
            return "".join(keep)

        for _ in range(50):
            if job.log_path.exists():
                break
            time.sleep(0.1)

        while True:
            try:
                with job.log_path.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(last_pos)
                    chunk_raw = f.read()
                    last_pos = f.tell()
            except Exception:
                chunk_raw = ""

            chunk = _filtrar_fases(chunk_raw)

            if chunk:
                yield (
                    f"id: {last_pos}\n"
                    "data: " + chunk.replace("\n", "\ndata: ") + "\n\n"
                )

            if job.done:
                if not chunk:
                    yield f"id: {last_pos}\ndata: __DONE__\n\n"
                    break

            time.sleep(0.35)

    return Response(generate(), mimetype="text/event-stream")


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

@app.get("/preview/<job_id>")
def preview(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return _page("Tarefa não encontrada", "<div class='card'><p>Tarefa não encontrada.</p></div>"), 404
        if not _is_job_owner(job, sess):
            return _page("Acesso negado", "<div class='card'><p class='status-err'>Sem permissões.</p></div>"), 403

    if not job.done or not job.ok:
        return redirect(url_for("progress", job_id=job_id))

    # Ler preview_payload
    payload_path = job.run_dir / "preview_payload.json"
    if not payload_path.exists():
        return _page("Erro", "<div class='card'><p class='status-err'>Dados de preview não encontrados.</p></div>"), 500
    payload = json.loads(payload_path.read_text(encoding="utf-8"))

    # Suporta jobs antigos (parecer_html) e novos (parecer_texto)
    parecer_texto = payload.get("parecer_texto") or payload.get("parecer_html", "")
    ce_nome = payload.get("ce_nome", job.ce_nome)
    ano_letivo = payload.get("ano_letivo", job.ano_letivo)

    # URL do relatório no SIGARRA (versão impressão)
    _pv = job.pv_id or ""
    if _pv.startswith("3c:"):
        _relatorio_url = f"https://sigarra.up.pt/feup/pt/relcur_geral.rel3c_edit?pv_id={_pv[3:]}&pv_print_ver=S"
    elif _pv:
        _relatorio_url = f"https://sigarra.up.pt/feup/pt/relcur_geral.proc_edit?pv_id={_pv}&pv_print_ver=S"
    else:
        _relatorio_url = ""

    csrf = _get_csrf_token()

    _link_relatorio = (
        f'<a href="{_relatorio_url}" target="_blank" rel="noopener">Ver relatório no SIGARRA</a>'
        if _relatorio_url else ""
    )

    # Botão de submissão: login por password ou admin; perspetiva CC/CP/CA
    _pode_submeter = (
        (_is_admin(sess) or flask_session.get("login_method") == "password")
        and (job.perspetiva or "").upper() in ("CC", "CP", "CA")
        and bool(job.pv_id)
    )
    # Verificar se já existe parecer no SIGARRA (aviso antes de sobrescrever)
    _parecer_existente = ""
    if _pode_submeter:
        try:
            _parecer_existente = obter_parecer_atual_sigarra(sess, job.pv_id, job.perspetiva)
        except Exception:
            pass
    _aviso_existente = (
        f'<div class="status-warn">&#9888; Já existe um parecer do '
        f'{_esc((job.perspetiva or "").upper())} guardado no SIGARRA. A submissão irá substituí-lo.</div>'
        if _parecer_existente else ""
    )
    _confirm_msg = (
        "Já existe um parecer guardado no SIGARRA. Confirma que pretende substituí-lo?"
        if _parecer_existente else
        "Confirma a submissão do parecer no SIGARRA?"
    )
    _btn_submeter = (
        f'<button type="submit" name="action" value="submeter_sigarra" class="btn"'
        f' onclick="return confirm({json.dumps(_confirm_msg)});"'
        f' id="btn-submeter">Submeter no SIGARRA</button>'
        if _pode_submeter else ""
    )

    body = f"""
    <div class="card">
      {_ce_titulo_html(ce_nome, ano_letivo)}
      <div class="muted">Parecer gerado — reveja e edite conforme necessário.</div>
      {(f'<p>{_link_relatorio}</p>') if _link_relatorio else ""}
    </div>

    <form method="post" action="{url_for('download_parecer', job_id=job_id)}" id="form-parecer">
      <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
          <h3 style="margin:0;">Parecer</h3>
          <span id="counter-parecer" style="font-size:0.85em; color:#888;"></span>
        </div>
        <textarea name="field_parecer" id="field_parecer" rows="22"
                  style="width:100%;box-sizing:border-box;font-family:inherit;font-size:0.96em;line-height:1.6;padding:10px;border:1px solid var(--line);border-radius:8px;resize:vertical;"
                  >{_esc(parecer_texto)}</textarea>
      </div>
      <div class="card">
        {_aviso_existente}
        <div style="display:flex; gap:16px; align-items:center; flex-wrap:wrap;">
          {_btn_submeter}
          <button type="submit" name="action" value="download_txt"
                  style="background:none;border:none;padding:0;color:var(--muted);cursor:pointer;font-size:inherit;text-decoration:underline;">Guardar texto</button>
          {'<a style="color:var(--muted);" href="' + url_for("encaminhar_get", job_id=job_id) + '">Encaminhar para revisão</a>' if _encaminhamento_ativo() else ''}
          <a style="color:var(--muted);" href="{url_for('download_zip', job_id=job_id)}">Exportar dados (.zip)</a>
        </div>
      </div>
    </form>
    <script>
    (function() {{
      var ta = document.getElementById('field_parecer');
      var counter = document.getElementById('counter-parecer');
      var btnSub = document.getElementById('btn-submeter');
      function update() {{
        var len = ta.value.length;
        counter.textContent = len + ' / 10000';
        counter.style.color = len > 10000 ? '#c00' : '#888';
        if (btnSub) btnSub.disabled = len > 10000;
      }}
      ta.addEventListener('input', update);
      update();
    }})();
    </script>
    """

    return _page("Parecer", body, step=3)


@app.post("/download/<job_id>/parecer")
def download_parecer(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))
    _require_csrf()

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job or not _is_job_owner(job, sess):
            abort(403)

    action = request.form.get("action", "download_txt")
    parecer_texto = request.form.get("field_parecer", "").strip()
    ce_slug = re.sub(r"[^a-z0-9]+", "-", (job.ce_nome or "ce").lower()).strip("-")

    if action == "submeter_sigarra":
        # Submissão direta ao SIGARRA: requer password ou admin
        if not _is_admin(sess) and flask_session.get("login_method") != "password":
            return _page("Submissão não disponível", """
            <div class="card">
              <p class="status-err">A submissão direta ao SIGARRA requer autenticação por password SIGARRA.</p>
              <p><a href="/login">Fazer login por password</a></p>
            </div>"""), 403
        try:
            submeter_parecer_sigarra(sess, job.pv_id, job.perspetiva, parecer_texto)
        except PermissionError as e:
            return _page("Erro na submissão", f"""
            <div class="card">
              <p class="status-err">Sem permissão para submeter no SIGARRA: {_esc(str(e))}</p>
              <p><a href="{url_for('resultado', job_id=job_id)}">Voltar ao parecer</a></p>
            </div>"""), 403
        except Exception as e:
            return _page("Erro na submissão", f"""
            <div class="card">
              <p class="status-err">Erro ao submeter no SIGARRA: {_esc(str(e))}</p>
              <p><a href="{url_for('resultado', job_id=job_id)}">Voltar ao parecer</a></p>
            </div>"""), 500
        flask_session[f"submitted_{job_id}"] = True
        return redirect(url_for("submissao_get", job_id=job_id))

    # Download como ficheiro de texto
    filename = f"parecer_{ce_slug}_{job.ano_letivo or 'na'}.txt".replace("/", "-")
    return Response(
        parecer_texto,
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Passo 4 — Submissão concluída + Notificar para revisão
# ---------------------------------------------------------------------------

def _url_edit_sigarra(pv_id: str) -> str:
    """URL da página de edição do relatório no SIGARRA (sem pv_print_ver)."""
    if pv_id.startswith("3c:"):
        return f"https://sigarra.up.pt/feup/pt/relcur_geral.rel3c_edit?pv_id={pv_id[3:]}"
    return f"https://sigarra.up.pt/feup/pt/relcur_geral.proc_edit?pv_id={pv_id}"


@app.get("/resultado/<job_id>/submissao")
def submissao_get(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job or not _is_job_owner(job, sess):
            abort(403)

    submitted = flask_session.pop(f"submitted_{job_id}", False)
    _pv = job.pv_id or ""
    _url_edit = _url_edit_sigarra(_pv) if _pv else ""
    _link_ver = (
        f'<p><a href="{_url_edit}" target="_blank" rel="noopener">Ver/editar parecer no SIGARRA</a></p>'
        if _url_edit else ""
    )
    _status_msg = '<p class="status-ok">Parecer submetido com sucesso no SIGARRA.</p>' if submitted else ""

    _notif_html = ""
    if _resend_api_key() and _url_edit:
        csrf = _get_csrf_token()
        _orgao_artigo = {"CC": "do", "CP": "do", "CA": "da"}.get((job.perspetiva or "").upper(), "do")
        _orgao_label = {
            "CC": "Conselho Científico",
            "CP": "Conselho Pedagógico",
            "CA": "Comissão de Acompanhamento",
        }.get((job.perspetiva or "").upper(), job.perspetiva or "")
        _notif_html = f"""
        <hr style="margin:20px 0;">
        <h3 style="margin:0 0 10px;">Notificar para revisão</h3>
        <p style="margin:0 0 12px;font-size:0.95em;">Envia um email a um membro {_esc(_orgao_artigo)} {_esc(_orgao_label)} com o link para o parecer no SIGARRA.</p>
        <form method="post" action="{url_for('notificar_post', job_id=job_id)}">
          <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
          <div class="row" style="align-items:center;gap:10px;max-width:420px;">
            <label style="min-width:80px;">Email UP:</label>
            <input name="notif_email" type="email" placeholder="upNNNNNN@up.pt"
                   pattern="up\\d{{6,9}}@(?:[\\w-]+\\.)*up\\.pt"
                   title="Email institucional UP (up seguido de número)"
                   style="flex:1;" required>
            <button type="submit">Notificar</button>
          </div>
        </form>"""

    body = f"""
    <div class="card">
      {_ce_titulo_html(job.ce_nome, job.ano_letivo)}
      {_status_msg}
      {_link_ver}
      {_notif_html}
      <p style="margin-top:20px;"><a href="{url_for('ces')}">Voltar ao início</a></p>
    </div>"""
    return _page("Submissão", body, step=5)


@app.post("/resultado/<job_id>/notificar")
@_limiter.limit("10 per minute")
def notificar_post(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))
    _require_csrf()
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job or not _is_job_owner(job, sess):
            abort(403)

    notif_email = request.form.get("notif_email", "").strip().lower()
    # Extrair código do email UP
    _m = re.match(r"^up(\d{6,9})@(?:[\w-]+\.)*up\.pt$", notif_email)
    if not _m:
        return _page("Notificação", f"""
        <div class="card">
          <p class="status-err">Email inválido: deve ter o formato <code>upNNNNNN@up.pt</code>.</p>
          <p><a href="{url_for('submissao_get', job_id=job_id)}">Voltar</a></p>
        </div>"""), 400
    dest_codigo = _m.group(1)

    # Validar que o destinatário tem permissão (ou é admin)
    if not _is_admin(sess) and not _reviewer_tem_permissao(dest_codigo, job.cur_id, job.perspetiva):
        return _page("Notificação", f"""
        <div class="card">
          <p class="status-err">O destinatário não tem cargo que permita emitir parecer de
          {_esc((job.perspetiva or "").upper())} para este ciclo de estudos.</p>
          <p><a href="{url_for('submissao_get', job_id=job_id)}">Voltar</a></p>
        </div>"""), 403

    # Nome do emissor
    eff = _effective_codigo(sess)
    _cargos_emissor = obter_cargos_docente(sess, eff)
    emissor_nome = _cargos_emissor.get("nome") or eff or "utilizador"

    # Link de edição no SIGARRA
    _pv = job.pv_id or ""
    _url_edit = _url_edit_sigarra(_pv) if _pv else ""
    _orgao_label = {
        "CC": "Conselho Científico",
        "CP": "Conselho Pedagógico",
        "CA": "Comissão de Acompanhamento",
    }.get((job.perspetiva or "").upper(), job.perspetiva or "")
    _orgao_artigo = {"CC": "do", "CP": "do", "CA": "da"}.get((job.perspetiva or "").upper(), "do")

    # Enviar email
    resend_key = _resend_api_key()
    _from = os.environ.get("RESEND_FROM", "noreply@ce.uc-reports.com")
    assunto = f"Parecer {_orgao_label} — {job.ce_nome} {job.ano_letivo} — notificação para revisão"
    corpo = (
        f"{emissor_nome} submeteu o parecer {_orgao_artigo} {_orgao_label} relativo ao ciclo de estudos "
        f'"{job.ce_nome}", ano letivo {job.ano_letivo}, e notifica-o para revisão.\n\n'
    )
    if _url_edit:
        corpo += f"Pode consultar e editar o parecer diretamente no SIGARRA:\n{_url_edit}\n\n"
    corpo += "Esta mensagem foi enviada automaticamente pelo sistema de pareceres de CEs da FEUP."

    try:
        _resend_body = json.dumps({
            "from": _from,
            "to": [notif_email],
            "subject": assunto,
            "text": corpo,
        }).encode()
        _resend_req = _urllib_req.Request(
            "https://api.resend.com/emails",
            data=_resend_body,
            headers={
                "Authorization": f"Bearer {resend_key}",
                "Content-Type": "application/json",
                "User-Agent": "ucreports/1.0",
            },
        )
        with _urllib_req.urlopen(_resend_req, timeout=15) as _r:
            _r.read()
    except Exception as e:
        app.logger.warning("notificar_post: erro ao enviar email: %s", e)
        return _page("Notificação", f"""
        <div class="card">
          <p class="status-err">Erro ao enviar email. Tente mais tarde.</p>
          <p><a href="{url_for('submissao_get', job_id=job_id)}">Voltar</a></p>
        </div>"""), 500

    return _page("Notificação enviada", f"""
    <div class="card">
      {_ce_titulo_html(job.ce_nome, job.ano_letivo)}
      <p class="status-ok">Email de notificação enviado para {_esc(notif_email)}.</p>
      <p><a href="{url_for('submissao_get', job_id=job_id)}">Voltar</a></p>
    </div>""", step=4)


# ---------------------------------------------------------------------------
# Download ZIP
# ---------------------------------------------------------------------------

@app.get("/download/<job_id>.zip")
def download_zip(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return _page("Download", "<div class='card'><p>Tarefa não encontrada.</p></div>"), 404
        if not _is_job_owner(job, sess):
            abort(403)

    if not job.run_dir or not job.run_dir.is_dir():
        return _page("Download", "<div class='card'><p>Dados não disponíveis.</p></div>"), 404

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(job.run_dir.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(job.run_dir))
    buf.seek(0)

    ce_slug = re.sub(r"[^a-z0-9]+", "-", (job.ce_nome or "ce").lower()).strip("-")
    filename = f"parecer_{ce_slug}_{job.ano_letivo or 'na'}.zip".replace("/", "-")
    return send_file(buf, mimetype="application/zip", as_attachment=True, download_name=filename)


# ---------------------------------------------------------------------------
# Encaminhar parecer para revisão
# ---------------------------------------------------------------------------

@app.get("/resultado/<job_id>/encaminhar")
def encaminhar_get(job_id: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login", next=request.url))

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if not job or not job.done or not job.ok:
        return _page("Erro", "<div class='card'><p class='status-err'>Tarefa não encontrada ou ainda em curso.</p></div>"), 404
    if not _is_job_owner(job, sess):
        abort(403)

    csrf = _get_csrf_token()
    perspetiva_label = _PERSPETIVA_LABELS_WEB.get(job.perspetiva, job.perspetiva)
    body = f"""
    <div class="card">
      {_ce_titulo_html(job.ce_nome, job.ano_letivo)}
      <p class="muted">Perspetiva: {_esc(perspetiva_label)}</p>
      <form method="post" action="{url_for('encaminhar_post', job_id=job_id)}" style="margin-top:14px;">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <div class="form-row-inline">
          <label for="reviewer_email">Email UP institucional do revisor:</label>
          <input type="email" name="reviewer_email" id="reviewer_email" required
                 placeholder="upNNNNNN@...up.pt" style="width:240px;">
        </div>
        <div class="form-row-inline" style="align-items:flex-start; margin-top:10px;">
          <label for="mensagem" style="padding-top:6px;">Mensagem (opcional):</label>
          <textarea name="mensagem" id="mensagem" rows="3" maxlength="500"
                    style="width:360px; font-size:0.93em; resize:vertical;"
                    placeholder="Contexto ou aspetos a considerar na revisão..."></textarea>
        </div>
        <div class="row" style="justify-content:flex-start; margin-top:14px; gap:10px;">
          <button type="submit" class="btn">Enviar pedido de revisão</button>
          <a class="btn btn-secondary" href="{url_for('preview', job_id=job_id)}">Cancelar</a>
        </div>
      </form>
    </div>
    """
    return _page("Encaminhar para revisão", body)


@app.post("/resultado/<job_id>/encaminhar")
def encaminhar_post(job_id: str):
    _require_csrf()
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if not job or not job.done or not job.ok:
        return _page("Erro", "<div class='card'><p class='status-err'>Tarefa não encontrada.</p></div>"), 404
    if not _is_job_owner(job, sess):
        abort(403)

    reviewer_email = request.form.get("reviewer_email", "").strip().lower()
    mensagem = request.form.get("mensagem", "").strip()[:500]

    reviewer_code = _codigo_de_email_otp(reviewer_email)
    if not reviewer_code:
        csrf = _get_csrf_token()
        perspetiva_label = _PERSPETIVA_LABELS_WEB.get(job.perspetiva, job.perspetiva)
        body = f"""
        <div class="card">
          {_ce_titulo_html(job.ce_nome, job.ano_letivo)}
          <p class="status-err">Email não reconhecido. Use um email UP no formato upNNNNNN@up.pt.</p>
          <p><a href="{url_for('encaminhar_get', job_id=job_id)}">Tentar novamente</a></p>
        </div>"""
        return _page("Encaminhar para revisão", body), 400

    if not _is_admin(sess) and not _reviewer_tem_permissao(reviewer_code, job.cur_id, job.perspetiva):
        perspetiva_label = _PERSPETIVA_LABELS_WEB.get(job.perspetiva, job.perspetiva)
        body = f"""
        <div class="card">
          {_ce_titulo_html(job.ce_nome, job.ano_letivo)}
          <p class="status-err">O utilizador <b>{_esc(reviewer_email)}</b> não tem permissão para emitir parecer
          na perspetiva <b>{_esc(perspetiva_label)}</b> para este ciclo de estudos.</p>
          <p><a href="{url_for('encaminhar_get', job_id=job_id)}">Tentar com outro email</a></p>
        </div>"""
        return _page("Encaminhar para revisão", body), 403

    owner_code_eff = _effective_codigo(sess)
    try:
        server_sess_enc = _get_server_session()
        owner_cargos = obter_cargos_docente(server_sess_enc, owner_code_eff)
        owner_nome = owner_cargos.get("nome", "")
    except Exception:
        owner_nome = ""
    owner_email_str = f"up{owner_code_eff}@up.pt" if owner_code_eff else ""

    try:
        token = _create_review(job, reviewer_code, reviewer_email, mensagem,
                               owner_nome=owner_nome)
        _send_review_email(
            reviewer_email=reviewer_email,
            ce_nome=job.ce_nome,
            ano_letivo=job.ano_letivo,
            perspetiva=job.perspetiva,
            owner_code=owner_code_eff,
            owner_nome=owner_nome,
            owner_email=owner_email_str,
            token=token,
            mensagem=mensagem,
        )
    except Exception as e:
        body = f"""
        <div class="card">
          <p class="status-err">Erro ao enviar pedido de revisão: {_esc(str(e))}</p>
          <p><a href="{url_for('encaminhar_get', job_id=job_id)}">Tentar novamente</a></p>
        </div>"""
        return _page("Encaminhar para revisão", body), 500

    body = f"""
    <div class="card">
      {_ce_titulo_html(job.ce_nome, job.ano_letivo)}
      <p class="status-ok">Pedido de revisão enviado para <b>{_esc(reviewer_email)}</b>.</p>
      <p class="muted">O link é válido por {REVIEW_TTL_DAYS} dias.</p>
      <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar ao início</a></p>
    </div>"""
    return _page("Revisão enviada", body)


# ---------------------------------------------------------------------------
# Página de revisão (acesso por token)
# ---------------------------------------------------------------------------

@app.get("/revisao/<token>")
def revisao_get(token: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login", next=request.url))

    review = _get_review(token)
    if not review:
        return _page("Ligação inválida", """
        <div class='card'>
          <p class='status-err'>Esta ligação é inválida ou expirou.</p>
          <p><a href='/ces'>Ir para o início</a></p>
        </div>"""), 404

    eff_code = _effective_codigo(sess)
    if eff_code != review["reviewer_code"] and eff_code not in _admin_codes():
        return _page("Sem acesso", f"""
        <div class='card'>
          <p class='status-err'>Esta ligação de revisão é para outro utilizador.</p>
          <p class='muted'>Autenticado como: {_esc(eff_code)}</p>
          <p><a href='/ces'>Ir para o início</a></p>
        </div>"""), 403

    run_dir = Path(review["run_dir"])
    # Suporta novos (parecer.txt) e antigos (parecer.html)
    parecer_path = run_dir / "parecer.txt"
    if not parecer_path.exists():
        parecer_path = run_dir / "parecer.html"
    if not parecer_path.exists():
        return _page("Erro", "<div class='card'><p class='status-err'>O ficheiro de parecer não foi encontrado. O conteúdo pode ter expirado.</p></div>"), 404

    parecer_html = parecer_path.read_text(encoding="utf-8", errors="replace")
    ce_nome = review.get("ce_nome", "")
    ano_letivo = review.get("ano_letivo", "")
    perspetiva_label = _PERSPETIVA_LABELS_WEB.get(review.get("perspetiva", ""), "")
    owner_code = review.get("owner_code", "")
    owner_nome = review.get("owner_nome", "")
    reviewer_nome_saved = review.get("reviewer_nome", "")
    if not owner_nome or not reviewer_nome_saved:
        try:
            _srv = _get_server_session()
            if not owner_nome:
                owner_nome = obter_cargos_docente(_srv, owner_code).get("nome", "")
            if not reviewer_nome_saved:
                reviewer_nome_saved = obter_cargos_docente(_srv, eff_code).get("nome", "")
            _update_review_fields(token, owner_nome=owner_nome, reviewer_nome=reviewer_nome_saved)
        except Exception:
            pass
    owner_display = f"{_esc(owner_nome)} ({_esc(owner_code)})" if owner_nome else _esc(owner_code)
    csrf = _get_csrf_token()

    body = f"""
    <div class="card">
      {_ce_titulo_html(ce_nome, ano_letivo)}
      {'<div class="muted">Perspetiva: ' + _esc(perspetiva_label) + '</div>' if perspetiva_label else ''}
      <p class="muted" style="margin-top:6px;">Parecer enviado por <b>{owner_display}</b> para revisão.</p>
    </div>

    <form method="post" action="{url_for('revisao_download', token=token)}" id="form-revisao">
      <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
          <h3 style="margin:0;">Parecer</h3>
          <span id="counter-rev" style="font-size:0.85em; color:#888;"></span>
        </div>
        <textarea name="field_parecer" id="field_parecer_rev" rows="22"
                  style="width:100%;box-sizing:border-box;font-family:inherit;font-size:0.96em;line-height:1.6;padding:10px;border:1px solid var(--line);border-radius:8px;resize:vertical;"
                  >{_esc(parecer_html)}</textarea>
      </div>
      <div class="card">
        <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap;">
          <button type="submit" class="btn" name="action" value="download_txt"
                  onclick="window._rev_baixado=true;">Guardar texto</button>
          {'<span style="color:#ccc;">|</span><a href="' + url_for("encaminhar_revisao_get", token=token) + '" class="btn btn-secondary">Reencaminhar</a>' if _encaminhamento_ativo() else ''}
        </div>
      </div>
    </form>

    <form method="post" action="{url_for('revisao_concluir', token=token)}"
          onsubmit="if(window._rev_editado && !window._rev_baixado){{ return confirm('Efetuou alterações mas ainda não guardou o texto. Pretende mesmo assim concluir?'); }} return true;">
      <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
      <div class="card">
        <button type="submit" class="btn">Concluir revisão</button>
      </div>
    </form>

    <script>
    (function() {{
      var ta = document.getElementById('field_parecer_rev');
      var counter = document.getElementById('counter-rev');
      function update() {{
        var len = ta.value.length;
        counter.textContent = len + ' / 10000';
        counter.style.color = len > 10000 ? '#c00' : '#888';
      }}
      ta.addEventListener('input', function() {{ window._rev_editado = true; window._rev_baixado = false; update(); }});
      update();
    }})();
    </script>
    """
    return _page("Revisão de parecer", body)


@app.post("/revisao/<token>/download")
def revisao_download(token: str):
    _require_csrf()
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    review = _get_review(token)
    if not review:
        abort(404)

    eff_code = _effective_codigo(sess)
    if eff_code != review["reviewer_code"] and eff_code not in _admin_codes():
        abort(403)

    parecer_texto = request.form.get("field_parecer", "").strip()
    ce_nome = review.get("ce_nome", "ce")
    ano_letivo = review.get("ano_letivo", "na")
    ce_slug = re.sub(r"[^a-z0-9]+", "-", ce_nome.lower()).strip("-")
    filename = f"parecer_{ce_slug}_{ano_letivo}.txt".replace("/", "-")

    return Response(
        parecer_texto,
        mimetype="text/plain; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/revisao/<token>/concluir")
def revisao_concluir(token: str):
    _require_csrf()
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    review = _get_review(token)
    if not review:
        abort(404)

    eff_code = _effective_codigo(sess)
    if eff_code != review["reviewer_code"] and eff_code not in _admin_codes():
        abort(403)

    _conclude_review(token)

    # Notificar o autor original (nomes guardados no registo, sem chamar SIGARRA aqui)
    owner_email = review.get("owner_email", "")
    owner_code = review.get("owner_code", "")
    owner_nome = review.get("owner_nome", "")
    reviewer_nome = review.get("reviewer_nome", "")

    try:
        _send_conclusion_email(
            owner_email=owner_email,
            owner_nome=owner_nome,
            owner_code=owner_code,
            reviewer_code=eff_code,
            reviewer_nome=reviewer_nome,
            ce_nome=review.get("ce_nome", ""),
            ano_letivo=review.get("ano_letivo", ""),
            perspetiva=review.get("perspetiva", ""),
        )
    except Exception:
        pass  # Falha de email não bloqueia conclusão

    ce_nome = review.get("ce_nome", "")
    ano_letivo = review.get("ano_letivo", "")
    body = f"""
    <div class="card">
      {_ce_titulo_html(ce_nome, ano_letivo)}
      <p class="status-ok"><b>Revisão concluída.</b>{' Foi enviado email de confirmação para ' + _esc(owner_email) + '.' if owner_email else ''}</p>
      <p><a class="btn btn-secondary" href="{url_for('ces')}">Voltar ao início</a></p>
    </div>"""
    return _page("Revisão concluída", body)


@app.get("/revisao/<token>/encaminhar")
def encaminhar_revisao_get(token: str):
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login", next=request.url))

    review = _get_review(token)
    if not review:
        return _page("Ligação inválida", "<div class='card'><p class='status-err'>Esta ligação é inválida ou expirou.</p></div>"), 404

    eff_code = _effective_codigo(sess)
    if eff_code != review["reviewer_code"] and eff_code not in _admin_codes():
        abort(403)

    perspetiva_label = _PERSPETIVA_LABELS_WEB.get(review.get("perspetiva", ""), "")
    csrf = _get_csrf_token()
    body = f"""
    <div class="card">
      {_ce_titulo_html(review.get("ce_nome", ""), review.get("ano_letivo", ""))}
      {'<p class="muted">Perspetiva: ' + _esc(perspetiva_label) + '</p>' if perspetiva_label else ''}
      <form method="post" action="{url_for('encaminhar_revisao_post', token=token)}" style="margin-top:14px;">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <div class="form-row-inline">
          <label for="reviewer_email2">Email UP institucional do revisor:</label>
          <input type="email" name="reviewer_email" id="reviewer_email2" required
                 placeholder="upNNNNNN@...up.pt" style="width:240px;">
        </div>
        <div class="form-row-inline" style="align-items:flex-start; margin-top:10px;">
          <label for="mensagem2" style="padding-top:6px;">Mensagem (opcional):</label>
          <textarea name="mensagem" id="mensagem2" rows="3" maxlength="500"
                    style="width:360px; font-size:0.93em; resize:vertical;"
                    placeholder="Contexto ou aspetos a considerar na revisão..."></textarea>
        </div>
        <div class="row" style="justify-content:flex-start; margin-top:14px; gap:10px;">
          <button type="submit" class="btn">Enviar pedido de revisão</button>
          <a class="btn btn-secondary" href="{url_for('revisao_get', token=token)}">Cancelar</a>
        </div>
      </form>
    </div>
    """
    return _page("Reencaminhar para revisão", body)


@app.post("/revisao/<token>/encaminhar")
def encaminhar_revisao_post(token: str):
    _require_csrf()
    sess = _get_sigarra_session()
    if not sess:
        return redirect(url_for("login"))

    review = _get_review(token)
    if not review:
        abort(404)

    eff_code = _effective_codigo(sess)
    if eff_code != review["reviewer_code"] and eff_code not in _admin_codes():
        abort(403)

    reviewer_email = request.form.get("reviewer_email", "").strip().lower()
    mensagem = request.form.get("mensagem", "").strip()[:500]

    reviewer_code = _codigo_de_email_otp(reviewer_email)
    if not reviewer_code:
        body = f"""
        <div class="card">
          <p class="status-err">Email não reconhecido. Use um email UP no formato upNNNNNN@up.pt.</p>
          <p><a href="{url_for('encaminhar_revisao_get', token=token)}">Tentar novamente</a></p>
        </div>"""
        return _page("Reencaminhar para revisão", body), 400

    perspetiva = review.get("perspetiva", "")
    cur_id = review.get("cur_id", "")
    if not _is_admin(sess) and not _reviewer_tem_permissao(reviewer_code, cur_id, perspetiva):
        perspetiva_label = _PERSPETIVA_LABELS_WEB.get(perspetiva, perspetiva)
        body = f"""
        <div class="card">
          <p class="status-err">O utilizador <b>{_esc(reviewer_email)}</b> não tem permissão para emitir parecer
          na perspetiva <b>{_esc(perspetiva_label)}</b> para este ciclo de estudos.</p>
          <p><a href="{url_for('encaminhar_revisao_get', token=token)}">Tentar com outro email</a></p>
        </div>"""
        return _page("Reencaminhar para revisão", body), 403

    # Criar novo review baseado no original (mesmos metadados, novo token, novo reviewer)
    run_dir = Path(review["run_dir"])
    # Construir um objeto mínimo compatível com _create_review
    class _FakeJob:
        pass
    fake_job = _FakeJob()
    fake_job.job_id = review.get("job_id", "")
    fake_job.run_dir = run_dir
    fake_job.ce_nome = review.get("ce_nome", "")
    fake_job.ano_letivo = review.get("ano_letivo", "")
    fake_job.perspetiva = perspetiva
    fake_job.pv_id = review.get("pv_id", "")
    fake_job.cur_id = cur_id
    fake_job.user_code = eff_code

    try:
        server_sess_rev = _get_server_session()
        rev_owner_cargos = obter_cargos_docente(server_sess_rev, eff_code)
        rev_owner_nome = rev_owner_cargos.get("nome", "")
    except Exception:
        rev_owner_nome = ""
    rev_owner_email = f"up{eff_code}@up.pt" if eff_code else ""

    try:
        new_token = _create_review(fake_job, reviewer_code, reviewer_email, mensagem,
                                   owner_nome=rev_owner_nome)
        _send_review_email(
            reviewer_email=reviewer_email,
            ce_nome=review.get("ce_nome", ""),
            ano_letivo=review.get("ano_letivo", ""),
            perspetiva=perspetiva,
            owner_code=eff_code,
            owner_nome=rev_owner_nome,
            owner_email=rev_owner_email,
            token=new_token,
            mensagem=mensagem,
        )
    except Exception as e:
        body = f"""
        <div class="card">
          <p class="status-err">Erro ao enviar pedido de revisão: {_esc(str(e))}</p>
          <p><a href="{url_for('encaminhar_revisao_get', token=token)}">Tentar novamente</a></p>
        </div>"""
        return _page("Reencaminhar para revisão", body), 500

    body = f"""
    <div class="card">
      {_ce_titulo_html(review.get("ce_nome", ""), review.get("ano_letivo", ""))}
      <p class="status-ok">Pedido de revisão enviado para <b>{_esc(reviewer_email)}</b>.</p>
      <p class="muted">O link é válido por {REVIEW_TTL_DAYS} dias.</p>
      <p><a class="btn btn-secondary" href="{url_for('revisao_get', token=token)}">Voltar ao parecer</a></p>
    </div>"""
    return _page("Revisão enviada", body)


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=False, threaded=True)
