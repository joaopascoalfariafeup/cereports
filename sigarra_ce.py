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
# Cargos relevantes do docente (para mostrar na página de seleção)
# ---------------------------------------------------------------------------

_EMPTY_CARGOS: dict = {
    "nome": "", "is_cp": False, "is_cc": False, "cac_cursos": [], "director_cur_ids": [],
}

_SIGLA_CACHE: dict[str, str] = {}


def _obter_sigla_curso(sess: SigarraSession, cur_id: str) -> str:
    """Obtém a sigla de um curso do SIGARRA (com cache em memória).

    Faz fetch de cur_geral.cur_view?pv_curso_id=NNN e procura a linha
    com label "Sigla". Devolve string vazia se não encontrar.
    """
    if cur_id in _SIGLA_CACHE:
        return _SIGLA_CACHE[cur_id]
    url = f"{SIGARRA_BASE}/cur_geral.cur_view?pv_curso_id={cur_id}"
    try:
        html_str = sess.fetch_html(url, timeout=15)
        soup = BeautifulSoup(html_str, "html.parser")
        for th in soup.find_all("th"):
            if re.search(r"\bsigla\b", th.get_text(strip=True), re.I):
                # th e td podem estar na mesma <tr> (não são irmãos directos entre si)
                row = th.find_parent("tr")
                td = th.find_next_sibling("td") or (row.find("td") if row else None)
                if td:
                    sigla = td.get_text(strip=True)
                    if sigla and 2 <= len(sigla) <= 16:
                        _SIGLA_CACHE[cur_id] = sigla
                        return sigla
    except Exception:
        pass
    _SIGLA_CACHE[cur_id] = ""
    return ""


def obter_cargos_docente(sess: SigarraSession, codigo_pessoal: str) -> dict:
    """Obtém cargos relevantes do docente da sua página pessoal SIGARRA.

    Faz fetch de func_geral.formview?p_codigo=<codigo> e extrai da tabela
    "Cargos" os papéis relevantes para emissão de pareceres de CEs.

    Returns dict com:
      nome            str   — nome completo do docente
      is_cp           bool  — membro/presidente do Conselho Pedagógico da FEUP
      is_cc           bool  — membro/presidente do Conselho Científico da FEUP
      cac_cursos      list  — [{"cur_id", "nome", "sigla", "papel"}] comissão de acompanhamento
      director_cur_ids list — [cur_id, ...] cursos em que é diretor (conflito)
    """
    if not codigo_pessoal:
        return dict(_EMPTY_CARGOS)
    url = f"{SIGARRA_BASE}/func_geral.formview?p_codigo={codigo_pessoal}"
    try:
        html_str = sess.fetch_html(url, timeout=20)
    except Exception:
        return dict(_EMPTY_CARGOS)

    soup = BeautifulSoup(html_str, "html.parser")

    # Nome do docente — h1 contém o nome institucional (ex: "FEUP"), usar h2
    nome = ""
    for tag in ("h2", "h3"):
        el = soup.find(tag)
        if el:
            t = el.get_text(strip=True)
            # Aceitar se parecer um nome próprio: tem espaço e >5 chars
            if " " in t and len(t) > 5:
                nome = t
                break
    if not nome:
        title_tag = soup.find("title")
        if title_tag:
            m = re.match(r"^([^|\-\(]+)", title_tag.get_text(strip=True))
            if m:
                candidate = m.group(1).strip()
                if " " in candidate and len(candidate) > 5:
                    nome = candidate

    # Localizar a secção "Cargos"
    cargos_h3 = soup.find("h3", string=re.compile(r"Cargos", re.I))
    if not cargos_h3:
        return {**_EMPTY_CARGOS, "nome": nome}
    table = cargos_h3.find_next("table")
    if not table:
        return {**_EMPTY_CARGOS, "nome": nome}

    is_cp = False
    is_cc = False
    cac_cursos: list[dict] = []
    director_cur_ids: list[str] = []

    for row in table.find_all("tr"):
        td = row.find("td", class_="k")
        if not td:
            continue
        text = td.get_text(separator=" ", strip=True)
        tl = text.lower()

        # Extrair link de curso, se presente
        a = td.find("a", href=re.compile(r"pv_curso_id=\d+", re.I))
        cur_id = cur_nome = None
        if a:
            m = re.search(r"pv_curso_id=(\d+)", a.get("href", ""), re.I)
            if m:
                cur_id = m.group(1)
                cur_nome = a.get_text(strip=True)

        if re.search(r"conselho\s+pedag[oó]gico", tl) and "comiss" not in tl:
            is_cp = True
        elif re.search(r"conselho\s+cient[ií]fico", tl) and "comiss" not in tl:
            is_cc = True
        elif re.search(r"comiss[aã]o\s+de\s+acompanhamento", tl) and cur_id:
            papel = "Presidente" if "presidente" in tl else "Membro"
            sigla = _obter_sigla_curso(sess, cur_id)
            cac_cursos.append({"cur_id": cur_id, "nome": cur_nome, "sigla": sigla, "papel": papel})
        elif re.search(r"diretor\s+de\s+(curso|mestrado|doutoramento|licenciatura)", tl) and cur_id:
            director_cur_ids.append(cur_id)

    return {
        "nome": nome,
        "is_cp": is_cp,
        "is_cc": is_cc,
        "cac_cursos": cac_cursos,
        "director_cur_ids": director_cur_ids,
    }


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
# HTML do relatório de um CE (versão impressão, autenticação obrigatória)
# ---------------------------------------------------------------------------

SIGARRA_RELCUR_PRINT_URL = (
    "https://sigarra.up.pt/feup/pt/relcur_geral.proc_edit?pv_id={}&pv_print_ver=S"
)

# Tags HTML a conservar na limpeza (estrutura semântica para o LLM)
_HTML_TAGS_MANTER_ATTRS = {"td": {"colspan", "rowspan"}, "th": {"colspan", "rowspan"}}
# Tags cujo conteúdo deve ser removido por completo
_HTML_TAGS_REMOVER = {
    "script", "style", "link", "meta", "noscript",
    "iframe", "object", "embed", "form", "input", "button",
}


def obter_relatorio_ce_html(pv_id: str, sessao: SigarraSession) -> str:
    """Obtém e limpa o HTML do relatório de CE a partir do SIGARRA.

    Faz fetch autenticado de
    https://sigarra.up.pt/feup/pt/relcur_geral.proc_edit?pv_id=NNN&pv_print_ver=S
    e devolve o HTML limpo de JS, CSS e atributos desnecessários,
    mantendo apenas a estrutura semântica (títulos, parágrafos, tabelas, listas).

    Args:
        pv_id:  Identificador do relatório (pv_id).
        sessao: Sessão autenticada no SIGARRA.

    Returns:
        String com o HTML limpo do relatório (conteúdo do <body>).

    Raises:
        ValueError: Se não for possível obter o relatório.
    """
    from bs4 import Comment

    url = SIGARRA_RELCUR_PRINT_URL.format(pv_id)
    try:
        html_str = sessao.fetch_html(url, timeout=30)
    except Exception as e:
        raise ValueError(f"Não foi possível obter o relatório pv_id={pv_id}: {e}") from e

    soup = BeautifulSoup(html_str, "html.parser")

    # 1. Remover tags indesejadas (e o seu conteúdo)
    for tag in soup.find_all(_HTML_TAGS_REMOVER):
        tag.decompose()

    # 2. Remover comentários HTML
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # 3. Limpar atributos: manter apenas colspan/rowspan em células de tabela
    for tag in soup.find_all(True):
        permitidos = _HTML_TAGS_MANTER_ATTRS.get(tag.name, set())
        attrs_apagar = [a for a in list(tag.attrs) if a not in permitidos]
        for a in attrs_apagar:
            del tag[a]

    # 4. Extrair o <body> (ou fallback para tudo)
    body = soup.find("body") or soup

    html_limpo = str(body)

    # 5. Comprimir espaços excessivos
    html_limpo = re.sub(r"[ \t]{2,}", " ", html_limpo)
    html_limpo = re.sub(r"\n{3,}", "\n\n", html_limpo)

    return html_limpo.strip()


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
