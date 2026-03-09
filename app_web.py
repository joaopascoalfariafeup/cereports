"""
Ponto de entrada Web para Assistente de Análise e Emissão de Pareceres sobre
Relatórios de Ciclos de Estudos (CEs) da FEUP.

- Login/password via formulário (SIGARRA)
- Upload de PDF do relatório de CE + seleção de ano letivo
- Análise por LLM em background
- Preview do parecer gerado com edição
"""

from __future__ import annotations

import html
import io
import json
import os
import re
import secrets
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import urllib.request as _urllib_req

from flask import Flask, request, session as flask_session, redirect, url_for, Response, abort, send_file
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from sigarra import SigarraSession, load_env
from logger import AuditoriaLogger
from ce_core import analisar_ce
from sigarra_ce import listar_ces_publicos, listar_relatorios_ce, obter_relatorio_ce_html, obter_cargos_docente


# Carregar .env antes de ler variáveis WEB_* no arranque do módulo
load_env()

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

WEB_VERBOSIDADE = int(os.environ.get("WEB_VERBOSIDADE", "0"))
WEB_OUTPUT_RETENTION_HOURS = float(os.environ.get("WEB_OUTPUT_RETENTION_HOURS", "2"))
WEB_OUTPUT_MAX_GB = float(os.environ.get("WEB_OUTPUT_MAX_GB", "2"))
MAX_RUNNING_JOBS = int(os.environ.get("WEB_MAX_RUNNING_JOBS", "4"))
MAX_JOBS = int(os.environ.get("WEB_MAX_JOBS", "20"))
_SESSION_TIMEOUT_S = 8 * 3600
_JOB_TIMEOUT_S = int(os.environ.get("WEB_JOB_TIMEOUT_S", "600"))  # 10 min

_DRAINING_FILE = _SCRIPT_DIR / ".draining"


@dataclass
class Tarefa:
    job_id: str
    log_path: Path
    started_at: float
    ce_nome: str = ""
    ano_letivo: str = ""
    pv_id: str = ""
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
    cutoff = time.time() - WEB_OUTPUT_RETENTION_HOURS * 3600
    if not OUTPUT_DIR.is_dir():
        return
    for entry in OUTPUT_DIR.iterdir():
        if entry.is_dir() and entry.stat().st_mtime < cutoff:
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

_STEPPER_LABELS = ["Seleção", "Geração", "Revisão"]


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
    .status-ok {{ color: #15803d; }}
    .status-err {{ color: #b91c1c; }}
    .status-run {{ color: #1d4ed8; }}
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
  </style>
</head>
<body>
<div class="container">
  <header class="app-header">
    <div class="app-brandrow">
      <span class="app-brand">Assistente de Apoio à Elaboração de Pareceres sobre Relatórios de Ciclos de Estudos</span>
    </div>
    <div class="app-subtitle">FEUP · Melhoria Contínua</div>
  </header>
  {stepper}
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
        if (submitBtn) { submitBtn.disabled = false; submitBtn.title = ''; }
      } else {
        original = block.innerHTML;
        block.contentEditable = 'true';
        block.focus();
        btnEdit.textContent = 'Guardar';
        btnEdit.classList.add('editing');
        if (btnCancel) { btnCancel.classList.add('visible'); }
        if (counter) { counter.classList.add('visible'); updateCounter(); }
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

  const curIdHidden = _byId('cur_id_hidden');
  function onCeChange() {
    const opt = ceSelect.options[ceSelect.selectedIndex];
    const curId = opt ? (opt.dataset.curId || '') : '';
    if (curIdHidden) curIdHidden.value = curId;
    loadYears(curId);
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
    body = f"""
    <div class="card">
      <form id="login-form" method="post" action="{url_for('login_post')}">
        <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
        <div class="row" style="align-items:center; gap:10px; max-width:400px;">
          <label style="width:78px; min-width:78px;">Utilizador:</label>
          <div class="input-with-suffix" style="width:220px;">
            <input name="login" autocomplete="username" required>
            <span class="input-suffix">@fe.up.pt</span>
          </div>
        </div>
        <div class="row" style="margin-top:10px; align-items:center; gap:10px; max-width:400px;">
          <label style="width:78px; min-width:78px;">Senha:</label>
          <input name="password" type="password" autocomplete="current-password" style="width:220px; max-width:100%;" required>
        </div>
        <div class="row" style="margin-top:14px;">
          <button id="btn-login" type="submit">Autenticar</button>
        </div>
        <p class="muted" style="margin-top:12px;">
          Ou <a href="{url_for('login_federado')}">Autenticação federada</a>
        </p>
        <p class="muted"><a href="{url_for('privacidade')}">Política de privacidade e proteção de dados</a></p>
      </form>
    </div>
    """
    return _page("Login", body)


@app.post("/login")
@_limiter.limit("10 per minute; 30 per hour")
def login_post():
    _require_csrf()
    login_val = request.form.get("login", "").strip()
    password = request.form.get("password", "")

    sess = SigarraSession()
    try:
        sess.autenticar(login=login_val, password=password)
    except Exception as e:
        return _page("Login", f"""
        <div class="card">
          <p><b>Falha na autenticação:</b> {_esc(e)}</p>
          <p><a href="{url_for('login')}">Voltar</a></p>
        </div>
        """)

    _set_sigarra_session(sess)
    flask_session["sigarra_login"] = login_val
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

      <h4>Credenciais e comunicação segura</h4>
      <p>
        A aplicação disponibiliza dois mecanismos de autenticação:
      </p>
      <ul>
        <li>
          <b>Autenticação direta (SIGARRA):</b> as credenciais introduzidas são usadas apenas para estabelecer
          uma sessão segura no SIGARRA.
          As credenciais não são guardadas em disco nem registadas em logs.
        </li>
        <li>
          <b>Autenticação federada (Shibboleth/SAML2):</b> o fluxo de autenticação é iniciado via
          Shibboleth/SAML2 com o Fornecedor de Identidade da Universidade do Porto (wayf.up.pt).
          Por razões técnicas (necessidade de sessão HTTP do lado do servidor para acesso à API SIGARRA),
          as credenciais introduzidas no formulário do IdP transitam pelo servidor desta aplicação antes
          de serem reencaminhadas para o IdP — tal como num proxy HTTPS. As credenciais são transmitidas
          exclusivamente sobre HTTPS, não são guardadas em disco e não são registadas em logs.
          Apenas a asserção SAML resultante é utilizada para estabelecer a sessão.
        </li>
      </ul>
      <p>
        Toda a comunicação entre o utilizador e a aplicação é protegida através de ligações cifradas (HTTPS/TLS),
        assegurando a confidencialidade e integridade dos dados em trânsito.
      </p>

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
        <a href="https://github.com/joaopascoalfariafeup/ucreports" target="_blank" rel="noopener">github.com/joaopascoalfariafeup/ucreports</a>.
      </p>

      
      <h4>Utilização de modelos de linguagem (LLM)</h4>
      <p>
        O parecer é gerado por um LLM (Anthropic Claude ou IAedu/GPT-4o). As garantias
        de privacidade dependem do fornecedor selecionado. O parecer gerado é apresentado
        ao utilizador para revisão antes de qualquer utilização.
      </p>
      <p class="muted">
        O código-fonte desta ferramenta é público em
        <a href="https://github.com/joaopascoalfariafeup/cereports" target="_blank" rel="noopener">github.com/joaopascoalfariafeup/cereports</a>.
      </p>
      <p class="muted"><a href="{url_for('login')}">Voltar ao login</a></p>
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
    _has_cargos = bool(_permit_tipos or _ca_ids)

    def _ce_permitido(ce: dict) -> tuple[bool, str]:
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
    docente_label = f'Docente: {_esc(nome_docente)} ({_esc(eff_code)})' if eff_code else ""

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
        cargos_html = f"""<div class="muted" style="margin:0 0 10px;font-size:0.9em;">
          {f'<strong>{docente_label}</strong>' if docente_label else ''}
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
                optgroups += (
                    f'<option value="{_esc(ce["nome"])}" data-cur-id="{_esc(ce["cur_id"])}"{sel}{disabled_attr}>'
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
          <select name="ano_letivo" id="ano_letivo" style="max-width:160px;"
                  data-fallback-anos="{anos_fallback_json}">
            <option value="" disabled selected>—</option>
          </select>
        </div>

        <input type="hidden" name="pv_id" id="pv_id" value="">

        <div class="form-row-inline">
          <label for="llm_choice_select">Modelo:</label>
          <select name="llm_choice" id="llm_choice_select" style="max-width:280px;">
            {llm_choice_option_tags}
          </select>
        </div>
        <p class="muted" style="margin:6px 0 0 160px;font-size:0.88em;">Sugestão: use o modelo gratuito para testes e o Claude Opus 4.6 para o parecer final.</p>

        <div class="row" style="justify-content:flex-start; margin-top:14px;">
          <button class="btn" type="submit">Gerar parecer</button>
        </div>
      </form>"""

    body = f"""
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

            # Fase 2 — análise por LLM (logada internamente por analisar_ce)
            analisar_ce(
                relatorio_html=relatorio_html,
                ce_nome=job.ce_nome,
                ano_letivo=job.ano_letivo,
                provider=job.llm_provider,
                modelo=job.llm_modelo,
                run_dir=job.run_dir,
                logger=log,
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

    if not ce_nome:
        return redirect(url_for("ces"))

    # Validação de permissões server-side
    eff_code = _effective_codigo(sess)
    if eff_code and cur_id:
        cargos = obter_cargos_docente(sess, eff_code)
        permit_tipos: set[str] = set()
        if cargos["is_cp"]:
            permit_tipos.update({"L", "M"})
        if cargos["is_cc"]:
            permit_tipos.update({"L", "M", "D"})
        ca_ids = {c["cur_id"] for c in cargos["cac_cursos"]}
        director_ids = {d["cur_id"] for d in cargos["director_cursos"]}
        has_cargos = bool(permit_tipos or ca_ids)
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

    if not pv_id or not re.match(r'^\d+$', pv_id):
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

    flask_session["last_llm_choice"] = llm_choice or f"{llm_provider}::{llm_modelo}"
    flask_session["last_ce_nome"] = ce_nome

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
        user_code=(sess.codigo_pessoal or "").strip(),
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

    parecer_html = payload.get("parecer_html", "")
    ce_nome = payload.get("ce_nome", job.ce_nome)
    ano_letivo = payload.get("ano_letivo", job.ano_letivo)

    csrf = _get_csrf_token()

    body = f"""
    <div class="card">
      {_ce_titulo_html(ce_nome, ano_letivo)}
      <div class="muted">Parecer gerado — reveja e utilize conforme necessário.</div>
    </div>

    <form method="post" action="{url_for('download_parecer', job_id=job_id)}" id="form-parecer">
      <input type="hidden" name="csrf_token" value="{_esc(csrf)}">
      <input type="hidden" name="field_parecer" id="field_parecer" value="{_esc(parecer_html)}">

      <div class="card">
        <div class="editable-header" data-editable-id="parecer">
          <h3>Parecer</h3>
          <div style="display:inline-flex; gap:8px; align-items:center;">
            <span class="edit-counter" id="counter-parecer"></span>
            <button type="button" class="btn-cancel-edit" id="cancel-parecer">Cancelar</button>
            <button type="button" class="btn-edit" id="edit-parecer">Editar</button>
          </div>
        </div>
        <div class="preview-html" id="parecer-block" data-field="parecer">{parecer_html}</div>
      </div>

      <div class="card">
        <div class="navbar">
          <div class="navbar-left">
            <button type="submit" class="btn" name="action" value="download_html">Guardar HTML</button>
          </div>
          <div class="navbar-right">
            <a class="muted" href="{url_for('download_zip', job_id=job_id)}">Exportar dados (.zip)</a>
          </div>
        </div>
      </div>
    </form>
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

    parecer_html = request.form.get("field_parecer", "").strip()
    ce_slug = re.sub(r"[^a-z0-9]+", "-", (job.ce_nome or "ce").lower()).strip("-")
    filename = f"parecer_{ce_slug}_{job.ano_letivo or 'na'}.html".replace("/", "-")

    html_doc = f"""<!doctype html>
<html lang="pt">
<head>
  <meta charset="utf-8">
  <title>Parecer — {html.escape(job.ce_nome)} — {html.escape(job.ano_letivo)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; line-height: 1.5; }}
    h3 {{ color: #1a1a1a; }}
    ul {{ margin: 8px 0 12px 20px; }}
    li {{ margin: 4px 0; }}
  </style>
</head>
<body>
{parecer_html}
</body>
</html>"""

    return Response(
        html_doc,
        mimetype="text/html; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
# Ponto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=False, threaded=True)
