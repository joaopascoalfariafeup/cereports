"""
Módulo de acesso ao SIGARRA para Ciclos de Estudos (CEs).

Funções específicas para listar CEs e obter relatórios pedagógicos.
"""

from __future__ import annotations

import re
import time
import urllib.request as _req
from bs4 import BeautifulSoup

from sigarra import SigarraSession, SIGARRA_BASE


# ---------------------------------------------------------------------------
# Lista pública de CEs (sem autenticação) — página oficial SIGARRA/FEUP
# ---------------------------------------------------------------------------

SIGARRA_CUR_INICIO_URL = "https://sigarra.up.pt/feup/pt/cur_geral.cur_inicio"

_CES_PUBLICOS_CACHE: list[dict] = []
_CES_PUBLICOS_CACHE_TS: float = 0.0
_CES_PUBLICOS_CACHE_TTL = 3600  # 1 hora


def listar_ces_publicos() -> list[dict]:
    """Obtém lista de CEs da página pública do SIGARRA (sem autenticação).

    Faz scraping de https://sigarra.up.pt/feup/pt/cur_geral.cur_inicio e
    extrai as listas de Licenciaturas (L_a), Mestrados (M_a) e
    Doutoramentos (D_a).

    Returns:
        Lista de dicts com campos: cur_id (str), nome (str), tipo ('L'|'M'|'D').
        Em caso de falha de rede devolve lista vazia.
        Resultado em cache durante 1 hora.
    """
    global _CES_PUBLICOS_CACHE, _CES_PUBLICOS_CACHE_TS

    now = time.time()
    if _CES_PUBLICOS_CACHE and (now - _CES_PUBLICOS_CACHE_TS) < _CES_PUBLICOS_CACHE_TTL:
        return _CES_PUBLICOS_CACHE

    try:
        req = _req.Request(
            SIGARRA_CUR_INICIO_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CEReports/1.0)"},
        )
        resp = _req.urlopen(req, timeout=15)
        charset = resp.headers.get_content_charset() or "utf-8"
        html_str = resp.read().decode(charset, errors="replace")
    except Exception:
        return _CES_PUBLICOS_CACHE  # devolve cache antigo se tiver, ou []

    soup = BeautifulSoup(html_str, "html.parser")

    # Cada tipo tem uma <ul id="X_a"> com <li><a href="...pv_curso_id=NNN">Nome</a>...
    TIPOS = [
        ("L", "L_a"),
        ("M", "M_a"),
        ("D", "D_a"),
    ]

    resultado: list[dict] = []
    for tipo, ul_id in TIPOS:
        ul = soup.find("ul", id=ul_id)
        if not ul:
            continue
        for li in ul.find_all("li"):
            a = li.find("a")
            if not a:
                continue
            href = a.get("href", "")
            m = re.search(r"pv_curso_id=(\d+)", href)
            if not m:
                continue
            nome = a.get_text(strip=True)
            if not nome:
                continue
            resultado.append({
                "cur_id": m.group(1),
                "nome": nome,
                "tipo": tipo,
            })

    if resultado:
        _CES_PUBLICOS_CACHE = resultado
        _CES_PUBLICOS_CACHE_TS = now

    return resultado


# ---------------------------------------------------------------------------
# Lista de relatórios disponíveis para um CE (sem autenticação)
# ---------------------------------------------------------------------------

SIGARRA_RELCURS_URL = "https://sigarra.up.pt/feup/pt/relcur_geral.show_relcurs?pv_curso_id={}"

_RELCURS_CACHE: dict[str, tuple[float, list[dict]]] = {}
_RELCURS_CACHE_TTL = 3600  # 1 hora


def listar_relatorios_ce(
    cur_id: str,
    sessao: "SigarraSession | None" = None,
) -> list[dict]:
    """Obtém lista de relatórios pedagógicos disponíveis para um curso no SIGARRA.

    Faz scraping (com sessão autenticada se fornecida) de
    https://sigarra.up.pt/feup/pt/relcur_geral.show_relcurs?pv_curso_id=NNN

    Args:
        cur_id:  Identificador do curso (pv_curso_id).
        sessao:  Sessão autenticada no SIGARRA; se None faz pedido anónimo.

    Returns:
        Lista de dicts com campos: pv_id (str), ano (str, ex: ``"2024"``).
        Ordenados do mais recente para o mais antigo.
        Em caso de falha de rede devolve lista vazia.
        Resultado em cache durante 1 hora por cur_id.
    """
    global _RELCURS_CACHE

    now = time.time()
    if cur_id in _RELCURS_CACHE:
        ts, cached = _RELCURS_CACHE[cur_id]
        if (now - ts) < _RELCURS_CACHE_TTL:
            return cached

    url = SIGARRA_RELCURS_URL.format(cur_id)
    try:
        if sessao is not None:
            html_str = sessao.fetch_html(url, timeout=15)
        else:
            req = _req.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; CEReports/1.0)"},
            )
            resp = _req.urlopen(req, timeout=15)
            charset = resp.headers.get_content_charset() or "utf-8"
            html_str = resp.read().decode(charset, errors="replace")
    except Exception:
        return []

    soup = BeautifulSoup(html_str, "html.parser")
    resultado: list[dict] = []

    for a in soup.find_all("a", href=re.compile(r"relcur_geral\.proc_edit\?pv_id=")):
        href = a.get("href", "")
        m_id = re.search(r"pv_id=(\d+)", href)
        if not m_id:
            continue
        texto = a.get_text(strip=True)
        m_ano = re.search(r"--\s*(\d{4})\s*$", texto)
        if not m_ano:
            continue
        resultado.append({
            "pv_id": m_id.group(1),
            "ano": m_ano.group(1),
        })

    # Mais recentes primeiro
    resultado.sort(key=lambda x: x["ano"], reverse=True)

    if resultado:
        _RELCURS_CACHE[cur_id] = (now, resultado)

    return resultado


# ---------------------------------------------------------------------------
# URLs dos CEs no SIGARRA
# ---------------------------------------------------------------------------

SIGARRA_CUR_LISTA_URL = f"{SIGARRA_BASE}/cur_geral.cur_list_view"
SIGARRA_CUR_REL_VIEW_URL = f"{SIGARRA_BASE}/cur_geral.rel_cur_view?pv_cur_id={{}}&pv_ano_letivo={{}}"
SIGARRA_CUR_REL_PDF_URL = f"{SIGARRA_BASE}/cur_geral.rel_cur_pdf?pv_cur_id={{}}&pv_ano_letivo={{}}"


# ---------------------------------------------------------------------------
# Funções de scraping
# ---------------------------------------------------------------------------

def listar_ces_sigarra(sessao: SigarraSession) -> list[dict]:
    """Lista ciclos de estudos disponíveis para o utilizador autenticado.

    Devolve lista de dicts com campos: cur_id, nome, sigla, grau.

    Nota: Implementação stub — a completar com scraping real do SIGARRA.
    """
    # TODO: implementar scraping de cur_geral.cur_list_view ou equivalente
    return []


def extrair_relatorio_ce_pdf(cur_id: str, ano_letivo: str, sessao: SigarraSession) -> bytes:
    """Obtém o PDF do relatório pedagógico de um CE a partir do SIGARRA.

    Args:
        cur_id:     Identificador do curso no SIGARRA (pv_cur_id).
        ano_letivo: Ano letivo no formato YYYY (ex: "2024").
        sessao:     Sessão autenticada no SIGARRA.

    Returns:
        Bytes do PDF do relatório.

    Raises:
        NotImplementedError: Se o scraping ainda não estiver implementado.
        ValueError: Se o relatório não for encontrado.
    """
    url = SIGARRA_CUR_REL_PDF_URL.format(cur_id, ano_letivo)
    try:
        import urllib.request as _req
        r = _req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = sessao.http_open(r, timeout=60, context=f"PDF relatório CE {cur_id}/{ano_letivo}")
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower():
            raise ValueError(
                f"Resposta não é um PDF (Content-Type: {content_type}). "
                f"O relatório pode não existir para este CE/ano."
            )
        return resp.read()
    except Exception as e:
        raise ValueError(
            f"Não foi possível obter o PDF do relatório CE {cur_id}/{ano_letivo}: {e}"
        ) from e
