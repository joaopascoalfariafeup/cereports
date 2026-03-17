"""
Módulo de acesso ao SIGARRA para Ciclos de Estudos (CEs).

Funções específicas para listar CEs e obter relatórios pedagógicos.
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
import urllib.request as _req
from bs4 import BeautifulSoup

_log = logging.getLogger(__name__)

from sigarra import SigarraSession, SIGARRA_BASE


# ---------------------------------------------------------------------------
# Cargos relevantes do docente (para mostrar na página de seleção)
# ---------------------------------------------------------------------------

_EMPTY_CARGOS: dict = {
    "nome": "", "is_cp": False, "is_cc": False, "cac_cursos": [], "director_cursos": [],
}

_SIGLA_CACHE: dict[str, str] = {}

# Cache da composição do CP e CC da FEUP (páginas públicas)
_CP_FEUP_URL = f"{SIGARRA_BASE}/web_base.gera_pagina?p_pagina=31720"
_CC_FEUP_URL = f"{SIGARRA_BASE}/web_base.gera_pagina?p_pagina=31719"
_CP_MEMBERS_CACHE: "tuple[float, list[dict]] | None" = None
_CC_MEMBERS_CACHE: "tuple[float, list[dict]] | None" = None
_CP_CACHE_TTL = 6 * 3600  # 6 horas


def _obter_membros_cp_feup(sess: "SigarraSession | None" = None) -> list[dict]:
    """Obtém membros do CP da FEUP com códigos e nomes (cache 6h).

    Returns list of {"codigo": str, "nome": str}.
    """
    global _CP_MEMBERS_CACHE
    now = time.time()
    if _CP_MEMBERS_CACHE is not None:
        ts, cached = _CP_MEMBERS_CACHE
        if now - ts < _CP_CACHE_TTL:
            return cached

    try:
        if sess is not None:
            html_str = sess.fetch_html(_CP_FEUP_URL, timeout=15)
        else:
            req = _req.Request(_CP_FEUP_URL, headers={"User-Agent": "Mozilla/5.0"})
            resp = _req.urlopen(req, timeout=15)
            html_str = resp.read().decode(
                resp.headers.get_content_charset() or "utf-8", errors="replace"
            )
    except Exception:
        return []

    soup = BeautifulSoup(html_str, "html.parser")
    membros: list[dict] = []
    codigos_vistos: set[str] = set()

    # Membros docentes: func_geral.formview?p_codigo=NNN
    for a in soup.find_all("a", href=re.compile(r"func_geral\.formview\?p_codigo=\d+", re.I)):
        m = re.search(r"p_codigo=(\d+)", a.get("href", ""), re.I)
        if m:
            codigo = m.group(1)
            if codigo not in codigos_vistos:
                codigos_vistos.add(codigo)
                membros.append({"codigo": codigo, "nome": a.get_text(strip=True)})

    # Membros estudantes: fest_geral.cursos_list?pv_num_unico=NNN
    for a in soup.find_all("a", href=re.compile(r"fest_geral\.cursos_list\?pv_num_unico=\d+", re.I)):
        m = re.search(r"pv_num_unico=(\d+)", a.get("href", ""), re.I)
        if m:
            codigo = m.group(1)
            if codigo not in codigos_vistos:
                codigos_vistos.add(codigo)
                membros.append({"codigo": codigo, "nome": a.get_text(strip=True)})

    _CP_MEMBERS_CACHE = (now, membros)
    return membros


def _obter_codigos_cp_feup(sess: "SigarraSession | None" = None) -> frozenset[str]:
    """Obtém os códigos de todos os membros do CP da FEUP (com cache de 6h)."""
    return frozenset(m["codigo"] for m in _obter_membros_cp_feup(sess))


def _obter_membros_cc_feup(sess: "SigarraSession | None" = None) -> list[dict]:
    """Obtém membros do CC da FEUP com códigos e nomes (cache 6h).

    Faz scraping de web_base.gera_pagina?p_pagina=31719.
    Returns list of {"codigo": str, "nome": str}.
    """
    global _CC_MEMBERS_CACHE
    now = time.time()
    if _CC_MEMBERS_CACHE is not None:
        ts, cached = _CC_MEMBERS_CACHE
        if now - ts < _CP_CACHE_TTL:
            return cached

    try:
        if sess is not None:
            html_str = sess.fetch_html(_CC_FEUP_URL, timeout=15)
        else:
            req = _req.Request(_CC_FEUP_URL, headers={"User-Agent": "Mozilla/5.0"})
            resp = _req.urlopen(req, timeout=15)
            html_str = resp.read().decode(
                resp.headers.get_content_charset() or "utf-8", errors="replace"
            )
    except Exception:
        return []

    soup = BeautifulSoup(html_str, "html.parser")
    membros: list[dict] = []
    codigos_vistos: set[str] = set()

    # Todos os membros aparecem como func_geral.formview?p_codigo=NNN (case-insensitive)
    for a in soup.find_all("a", href=re.compile(r"func_geral\.formview\?p_codigo=\d+", re.I)):
        m = re.search(r"p_codigo=(\d+)", a.get("href", ""), re.I)
        if m:
            codigo = m.group(1)
            if codigo not in codigos_vistos:
                codigos_vistos.add(codigo)
                membros.append({"codigo": codigo, "nome": a.get_text(strip=True)})

    _CC_MEMBERS_CACHE = (now, membros)
    return membros


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
        # SIGARRA usa <td class="k"> como label (não <th>); pesquisar ambos
        for label_el in soup.find_all(["th", "td"]):
            if re.search(r"\bsigla\b", label_el.get_text(strip=True), re.I):
                row = label_el.find_parent("tr")
                # Próximo <td> irmão, ou qualquer <td> na mesma linha que não seja o label
                value_el = label_el.find_next_sibling("td")
                if not value_el and row:
                    value_el = next((t for t in row.find_all("td") if t is not label_el), None)
                if value_el:
                    sigla = value_el.get_text(strip=True)
                    if sigla and 2 <= len(sigla) <= 16:
                        _SIGLA_CACHE[cur_id] = sigla
                        return sigla
    except Exception:
        pass
    _SIGLA_CACHE[cur_id] = ""
    return ""


def _is_estudante(codigo: str) -> bool:
    """Código de estudante: tipicamente 9+ dígitos a começar com ano (20XX...)."""
    return bool(re.match(r"^20\d{7,}$", codigo))


def _obter_cargos_estudante(sess: SigarraSession, num_unico: str) -> dict:
    """Obtém cargos de um estudante (CP e CA).

    1. Verifica se o estudante é membro do CP via página pública da FEUP.
    2. Vai a fest_geral.cursos_list?pv_num_unico=NNN para obter cursos "A Frequentar".
    3. Para cada curso, verifica se o estudante está na CA via
       CUR_GERAL.CUR_COMISSAO_acomp_LIST?pv_curso_id=NNN.
    """
    url = f"{SIGARRA_BASE}/fest_geral.cursos_list?pv_num_unico={num_unico}"
    try:
        html_str = sess.fetch_html(url, timeout=20)
    except Exception:
        return dict(_EMPTY_CARGOS)

    soup = BeautifulSoup(html_str, "html.parser")

    # Nome — do título da página
    nome = ""
    title_tag = soup.find("title")
    if title_tag:
        raw = title_tag.get_text(strip=True)
        for part in re.split(r"\s*[-|]\s*", raw):
            part = part.strip()
            if " " in part and 6 < len(part) < 100 and not re.search(r"(sigarra|porto|feup)", part, re.I):
                nome = part
                break

    # Verificar CP via página pública
    is_cp = num_unico in _obter_codigos_cp_feup(sess)

    cac_cursos: list[dict] = []
    for div in soup.find_all("div", class_="estudante-lista-curso-activo"):
        # Verificar "A Frequentar"
        estado = None
        for row in div.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) == 2 and "estado" in cells[0].get_text(strip=True).lower():
                estado = cells[1].get_text(strip=True)
        if estado != "A Frequentar":
            continue

        # Obter cur_id
        a_curso = div.find("a", href=re.compile(r"cur_geral\.cur_view\?pv_curso_id=\d+", re.I))
        if not a_curso:
            continue
        m = re.search(r"pv_curso_id=(\d+)", a_curso.get("href", ""))
        if not m:
            continue
        cur_id = m.group(1)
        cur_nome = a_curso.get_text(strip=True)

        # Verificar membership na CA
        ca_url = f"{SIGARRA_BASE}/CUR_GERAL.CUR_COMISSAO_acomp_LIST?pv_curso_id={cur_id}"
        try:
            ca_html = sess.fetch_html(ca_url, timeout=15)
            ca_soup = BeautifulSoup(ca_html, "html.parser")
            ca_link = ca_soup.find("a", href=re.compile(rf"pv_num_unico={re.escape(num_unico)}", re.I))
            if not ca_link:
                continue
            papel = "Membro"
            row_ca = ca_link.find_parent("tr")
            if row_ca:
                td_papel = row_ca.find("td", class_="k")
                if td_papel:
                    papel = td_papel.get_text(strip=True) or "Membro"
                if not nome:
                    nome = ca_link.get_text(strip=True)
            sigla = _obter_sigla_curso(sess, cur_id)
            cac_cursos.append({"cur_id": cur_id, "nome": cur_nome, "sigla": sigla, "papel": papel})
        except Exception:
            continue

    return {**_EMPTY_CARGOS, "nome": nome, "is_cp": is_cp, "cac_cursos": cac_cursos}


def obter_cargos_docente(sess: SigarraSession, codigo_pessoal: str) -> dict:
    """Obtém cargos relevantes de um utilizador (docente ou estudante).

    Para estudantes (código 20XXXXXXX): vai à página de estudante e verifica CA.
    Para docentes: vai à página de cargos e extrai CP, CC, CA, diretor.

    Returns dict com:
      nome            str  — nome completo
      is_cp           bool — membro do Conselho Pedagógico da FEUP
      is_cc           bool — membro do Conselho Científico da FEUP
      cac_cursos      list — [{"cur_id", "nome", "sigla", "papel"}]
      director_cursos list — [{"cur_id", "nome", "sigla"}]
    """
    if not codigo_pessoal:
        return dict(_EMPTY_CARGOS)
    if _is_estudante(codigo_pessoal):
        return _obter_cargos_estudante(sess, codigo_pessoal)
    url = f"{SIGARRA_BASE}/func_geral.formview?p_codigo={codigo_pessoal}"
    try:
        html_str = sess.fetch_html(url, timeout=20)
    except Exception:
        return dict(_EMPTY_CARGOS)

    soup = BeautifulSoup(html_str, "html.parser")

    # Nome do docente — várias estratégias por ordem de fiabilidade
    nome = ""
    # 1. Título da página (ex: "João Carlos Pascoal Faria - SIGARRA")
    #    É a estratégia mais fiável porque reflete a página vista, não o utilizador logado.
    title_tag = soup.find("title")
    if title_tag:
        raw = title_tag.get_text(strip=True)
        for part in re.split(r"\s*[-|]\s*", raw):
            part = part.strip()
            if " " in part and 6 < len(part) < 100 and not re.search(r"(sigarra|porto|feup)", part, re.I):
                nome = part
                break
    # 2. h2 / h3 com aspecto de nome próprio (exclui "FEUP" e similares)
    if not nome:
        for tag in ("h2", "h3"):
            for el in soup.find_all(tag):
                t = el.get_text(strip=True)
                if " " in t and 6 < len(t) < 100 and not re.search(r"(sigarra|porto|feup)", t, re.I):
                    nome = t
                    break
            if nome:
                break
    # 3. Elemento com "nome" na classe (pode incluir o nav com o utilizador logado — último recurso)
    if not nome:
        for el in soup.find_all(class_=re.compile(r"nome", re.I)):
            t = el.get_text(strip=True)
            if " " in t and 6 < len(t) < 100:
                nome = t
                break

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
    director_cursos: list[dict] = []

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
            sigla = _obter_sigla_curso(sess, cur_id)
            director_cursos.append({"cur_id": cur_id, "nome": cur_nome, "sigla": sigla})

    # Diretores de curso são por inerência membros da CA — remover duplicados
    director_ids = {d["cur_id"] for d in director_cursos}
    cac_cursos = [c for c in cac_cursos if c["cur_id"] not in director_ids]

    return {
        "nome": nome,
        "is_cp": is_cp,
        "is_cc": is_cc,
        "cac_cursos": cac_cursos,
        "director_cursos": director_cursos,
    }


# ---------------------------------------------------------------------------
# Listagem de membros de órgãos (CP, CA) — para dropdown de notificação
# ---------------------------------------------------------------------------

def _obter_membros_ca(sess: SigarraSession, cur_id: str) -> list[dict]:
    """Obtém membros da Comissão de Acompanhamento de um curso.

    Faz scraping de CUR_GERAL.CUR_COMISSAO_acomp_LIST?pv_curso_id=NNN.
    Returns list of {"codigo": str, "nome": str}.
    """
    url = f"{SIGARRA_BASE}/CUR_GERAL.CUR_COMISSAO_acomp_LIST?pv_curso_id={cur_id}"
    try:
        html_str = sess.fetch_html(url, timeout=15)
    except Exception:
        return []

    soup = BeautifulSoup(html_str, "html.parser")
    membros: list[dict] = []
    codigos_vistos: set[str] = set()

    # Docentes: func_geral.formview?p_codigo=NNN
    for a in soup.find_all("a", href=re.compile(r"func_geral\.formview\?p_codigo=\d+", re.I)):
        m = re.search(r"p_codigo=(\d+)", a.get("href", ""), re.I)
        if m:
            codigo = m.group(1)
            if codigo not in codigos_vistos:
                codigos_vistos.add(codigo)
                membros.append({"codigo": codigo, "nome": a.get_text(strip=True)})

    # Estudantes: fest_geral.cursos_list?pv_num_unico=NNN
    for a in soup.find_all("a", href=re.compile(r"fest_geral\.cursos_list\?pv_num_unico=\d+", re.I)):
        m = re.search(r"pv_num_unico=(\d+)", a.get("href", ""), re.I)
        if m:
            codigo = m.group(1)
            if codigo not in codigos_vistos:
                codigos_vistos.add(codigo)
                membros.append({"codigo": codigo, "nome": a.get_text(strip=True)})

    return membros


def obter_diretores_curso(sess: SigarraSession, cur_id: str) -> list[str]:
    """Obtém códigos do diretor, codiretor e diretor adjunto de um curso.

    Faz scraping de cur_geral.cur_view?pv_curso_id=NNN.
    Returns lista de códigos pessoais (pode estar vazia).
    """
    url = f"{SIGARRA_BASE}/cur_geral.cur_view?pv_curso_id={cur_id}"
    try:
        html_str = sess.fetch_html(url, timeout=15)
    except Exception:
        return []

    soup = BeautifulSoup(html_str, "html.parser")
    # Procura labels "Diretor", "Codiretor", "Diretor Adjunto" numa tabela
    # e extrai códigos dos links (dois padrões de URL no SIGARRA):
    #   func_geral.formview?p_codigo=NNN
    #   vld_entidades_geral.entidade_pagina?pct_codigo=NNN
    _RE_CODIGO = re.compile(r"(?:p_codigo|pct_codigo)=(\d+)", re.I)
    codigos: list[str] = []
    vistos: set[str] = set()
    for label_el in soup.find_all(["th", "td"]):
        label_text = label_el.get_text(strip=True).lower()
        if re.search(r"\bdiret|\bcodiret", label_text):
            row = label_el.find_parent("tr")
            if row:
                for a in row.find_all("a", href=True):
                    m = _RE_CODIGO.search(a.get("href", ""))
                    if m and m.group(1) not in vistos:
                        vistos.add(m.group(1))
                        codigos.append(m.group(1))
    return codigos


def listar_membros_orgao(sess: SigarraSession, perspetiva: str, cur_id: str = "") -> list[dict]:
    """Lista membros do órgão correspondente à perspetiva.

    Returns list of {"codigo": str, "nome": str}, ordenada por nome.
    Para CC devolve lista vazia (URL da composição do CC da FEUP não conhecida).
    """
    persp = (perspetiva or "").upper()
    if persp == "CP":
        membros = list(_obter_membros_cp_feup(sess))
    elif persp == "CC":
        membros = list(_obter_membros_cc_feup(sess))
    elif persp == "CA":
        membros = _obter_membros_ca(sess, cur_id) if cur_id else []
    else:
        # DCE: auto-avaliação, sem notificação
        return []
    membros.sort(key=lambda m: unicodedata.normalize("NFKD", m.get("nome", ""))
                 .encode("ascii", "ignore").decode().lower())
    return membros


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
# Lista de relatórios disponíveis para um CE (requer autenticação)
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
    except Exception as _e:
        _log.warning("listar_relatorios_ce(%s): erro ao obter HTML: %s", cur_id, _e)
        return []

    soup = BeautifulSoup(html_str, "html.parser")
    resultado: list[dict] = []

    def _extrair_ano(texto: str) -> str | None:
        """Extrai ano letivo de início do texto do link (várias formatações)."""
        # "-- 2024" ou "-- 2023/24"
        m = re.search(r"--\s*(\d{4})(?:[/\-]\d{2,4})?\s*$", texto)
        if m:
            return m.group(1)
        # "2024/25" ou "2023-2024" isolado
        m = re.search(r"\b(20\d{2})[/\-]\d{2,4}\b", texto)
        if m:
            return m.group(1)
        # Ano isolado no final, ex: "Relatório 2024"
        m = re.search(r"\b(20\d{2})\s*$", texto)
        if m:
            return m.group(1)
        return None

    # Links do tipo relcur_geral.proc_edit?pv_id=NNN (1º/2º ciclos)
    # ou relcur_geral.rel3c_edit?pv_id=NNN (3º ciclo — doutoramentos)
    for a in soup.find_all("a", href=re.compile(r"relcur_geral\.(proc_edit|rel3c_edit)\?pv_id=", re.I)):
        href = a.get("href", "")
        m_id = re.search(r"pv_id=(\d+)", href, re.I)
        if not m_id:
            continue
        ano = _extrair_ano(a.get_text(strip=True))
        if not ano:
            continue
        # Prefixo "3c:" para doutoramentos — usado em obter_relatorio_ce_html
        is_3c = bool(re.search(r"rel3c_edit", href, re.I))
        pv_id = f"3c:{m_id.group(1)}" if is_3c else m_id.group(1)
        resultado.append({"pv_id": pv_id, "ano": ano})

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


def _extrair_highcharts(soup: "BeautifulSoup") -> None:
    """Extrai dados de gráficos Highcharts e injeta-os como tabela HTML legível.

    Procura scripts que chamam .highcharts({...}), extrai categorias e dados
    das séries, e substitui o div-alvo vazio por uma tabela com esses valores.
    Deve ser chamado antes de remover as tags <script>.
    """
    for script in soup.find_all("script"):
        js = script.string or ""
        if ".highcharts(" not in js:
            continue

        # Identificar o div-alvo: $("#div_id").highcharts(...)
        m_sel = re.search(r'\$\s*\(\s*["\']#([^"\']+)["\']\s*\)', js)
        if not m_sel:
            continue
        div_id = m_sel.group(1)
        target_div = soup.find(id=div_id)
        if not target_div:
            continue

        # Extrair categorias e dados da série
        m_cat = re.search(r'categories\s*:\s*\[([^\]]+)\]', js)
        m_data = re.search(r'\bdata\s*:\s*\[([^\]]+)\]', js)
        if not m_cat or not m_data:
            continue
        try:
            cats = [c.strip().strip('"\'') for c in m_cat.group(1).split(",")]
            vals = [float(v.strip()) for v in m_data.group(1).split(",")]
        except ValueError:
            continue
        if len(cats) != len(vals) or not cats:
            continue

        # Título do eixo X (label das colunas)
        m_xtitle = re.search(
            r'xAxis\b.*?title\s*:\s*\{[^}]*text\s*:\s*["\']([^"\']+)["\']',
            js, re.DOTALL,
        )
        x_label = m_xtitle.group(1) if m_xtitle else ""

        # Unidade / label dos valores (eixo Y)
        m_ytitle = re.search(
            r'yAxis\b.*?title\s*:\s*\{[^}]*text\s*:\s*["\']([^"\']+)["\']',
            js, re.DOTALL,
        )
        y_label = m_ytitle.group(1) if m_ytitle else "%"

        # Construir tabela
        target_div.clear()
        if x_label:
            p = soup.new_tag("p")
            p.string = f"{x_label} ({y_label}):"
            target_div.append(p)

        table = soup.new_tag("table")
        tr_h = soup.new_tag("tr")
        tr_d = soup.new_tag("tr")
        for cat, val in zip(cats, vals):
            th = soup.new_tag("th")
            th.string = str(cat)
            tr_h.append(th)
            td = soup.new_tag("td")
            td.string = f"{val:.1f}%"
            tr_d.append(td)
        table.append(tr_h)
        table.append(tr_d)
        target_div.append(table)


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

    # pv_id "3c:NNN" → doutoramento (rel3c_edit); NNN → 1º/2º ciclo (proc_edit)
    if pv_id.startswith("3c:"):
        real_id = pv_id[3:]
        url = f"{SIGARRA_BASE}/relcur_geral.rel3c_edit?pv_id={real_id}&pv_print_ver=S"
    else:
        url = SIGARRA_RELCUR_PRINT_URL.format(pv_id)
    try:
        html_str = sessao.fetch_html(url, timeout=30)
    except Exception as e:
        raise ValueError(f"Não foi possível obter o relatório pv_id={pv_id}: {e}") from e

    soup = BeautifulSoup(html_str, "html.parser")

    # 0. Converter inputs de texto com value em spans legíveis pelo LLM,
    #    antes de os remover no passo seguinte.
    for inp in soup.find_all("input", {"type": "text"}):
        val = (inp.get("value") or "").strip()
        span = soup.new_tag("span")
        span.string = val if val else "—"
        inp.replace_with(span)

    # 0b. Remover pareceres existentes no relatório atual para não
    #     influenciar a geração de novos pareceres pelo LLM.
    #     Vista edição: <div id="div_parecer_cc/cp/ca">
    for dp in soup.find_all("div", id=re.compile(r"^div_parecer_", re.I)):
        dp.decompose()
    #     Vista impressão: secção <h3>Pareceres</h3> + conteúdo até próximo <h3>
    for h3 in soup.find_all("h3"):
        if h3.get_text(strip=True) == "Pareceres":
            siblings_to_remove = []
            for sib in h3.next_siblings:
                if getattr(sib, "name", None) == "h3":
                    break
                siblings_to_remove.append(sib)
            for sib in siblings_to_remove:
                sib.extract()
            h3.decompose()
            break

    # 0c. Extrair dados de gráficos Highcharts antes de remover scripts.
    _extrair_highcharts(soup)

    # 1. Remover tags indesejadas (e o seu conteúdo)
    for tag in soup.find_all(_HTML_TAGS_REMOVER):
        tag.decompose()

    # 2. Remover comentários HTML
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # 2b. Sumarizar tabelas de corpo docente quando internos > threshold.
    #     Agrupa por (UO, Categoria, Grau) com contagem e somas de ETI.
    #     Quando internos são muitos, externos também são sumarizados (coerência).
    _CORPO_DOCENTE_THRESHOLD = 20
    from collections import defaultdict

    # Primeiro, verificar se internos excedem o threshold
    _sumarizar_todos = False
    for h4 in soup.find_all("h4"):
        if re.search(r"Corpo docente.*interno", h4.get_text(strip=True), re.I):
            t_int = h4.find_next("table")
            if t_int:
                n_rows = sum(1 for tr in t_int.find_all("tr")
                             if len(tr.find_all("span", class_="relcur_table")) >= 7)
                if n_rows > _CORPO_DOCENTE_THRESHOLD:
                    _sumarizar_todos = True
            break

    if _sumarizar_todos:
        for h4 in soup.find_all("h4"):
            if not re.search(r"Corpo docente", h4.get_text(strip=True), re.I):
                continue
            table = h4.find_next("table")
            if not table:
                continue
            # Recolher linhas de dados e Totais
            data_rows = []
            totais_row = None
            for tr in table.find_all("tr"):
                td_k = tr.find("td", class_="k")
                if td_k and re.search(r"^Totais$", td_k.get_text(strip=True)):
                    totais_row = tr
                    continue
                spans = tr.find_all("span", class_="relcur_table")
                if len(spans) >= 7:
                    data_rows.append(spans)
            if not data_rows:
                continue
            # Agrupar por (UO, Categoria, Grau)
            grupos: dict[tuple, dict] = defaultdict(lambda: {
                "n": 0, "regime_eti": 0.0, "esforco_ce": 0.0, "esforco_sem_ot": 0.0,
            })
            for spans in data_rows:
                uo = spans[1].get_text(strip=True) or "N/A"
                categoria = spans[2].get_text(strip=True) or "N/A"
                grau = spans[3].get_text(strip=True) or "N/A"
                key = (uo, categoria, grau)
                g = grupos[key]
                g["n"] += 1
                for field, idx in [("regime_eti", 5), ("esforco_ce", 6), ("esforco_sem_ot", 7)]:
                    txt = spans[idx].get_text(strip=True)
                    if txt:
                        try:
                            g[field] += float(txt.replace(",", "."))
                        except ValueError:
                            pass
            # Construir tabela sumarizada
            new_table = soup.new_tag("table")
            tr_h = soup.new_tag("tr")
            for col in ["UO", "Categoria", "Grau", "Nº Docentes",
                         "Regime de tempo na Instituição (ETI)",
                         "Esforço Docente no CE (ETI)",
                         "Esforço Docente sem OT (ETI)"]:
                th = soup.new_tag("th")
                th.string = col
                tr_h.append(th)
            new_table.append(tr_h)
            for (uo, cat, grau), g in sorted(grupos.items()):
                tr_d = soup.new_tag("tr")
                for val in [uo, cat, grau, str(g["n"]),
                            f"{g['regime_eti']:.2f}", f"{g['esforco_ce']:.3f}",
                            f"{g['esforco_sem_ot']:.3f}"]:
                    td = soup.new_tag("td")
                    td.string = val
                    tr_d.append(td)
                new_table.append(tr_d)
            if totais_row:
                # Reconstruir Totais para alinhar com novas colunas
                tds_orig = totais_row.find_all("td")
                # tds_orig: [Totais, #N(colspan=4), regime, esforço_ce, esforço_sem_ot]
                n_total = len(data_rows)
                regime_total = tds_orig[2].get_text(strip=True) if len(tds_orig) >= 3 else ""
                esforco_ce_total = tds_orig[3].get_text(strip=True) if len(tds_orig) >= 4 else ""
                esforco_ot_total = tds_orig[4].get_text(strip=True) if len(tds_orig) >= 5 else ""
                tr_t = soup.new_tag("tr")
                td_label = soup.new_tag("td", colspan="3")
                td_label.string = "Totais"
                tr_t.append(td_label)
                for val in [str(n_total), regime_total, esforco_ce_total, esforco_ot_total]:
                    td = soup.new_tag("td")
                    td.string = val
                    tr_t.append(td)
                new_table.append(tr_t)
            table.replace_with(new_table)

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
# Pareceres de anos anteriores
# ---------------------------------------------------------------------------

def extrair_pareceres_texto(html_str: str) -> str | None:
    """Extrai o texto dos pareceres de um HTML de relatório de CE (HTML original, não limpo).

    Procura divs com class ``div_parecer`` e extrai o conteúdo dos campos
    ``relcur_memo``, com a etiqueta do órgão emitente.

    Returns:
        String com os pareceres formatados, ou None se não houver nenhum.
    """
    soup = BeautifulSoup(html_str, "html.parser")

    # Abordagem 1: divs com id="div_parecer_*" (id é inequívoco; class matching
    # pode falhar com certos parsers HTML)
    textos: list[str] = []
    for div in soup.find_all("div", id=re.compile(r"^div_parecer_", re.I)):
        label_el = div.find("label")
        label = label_el.get_text(strip=True).rstrip(":") if label_el else ""
        # print view: <div class="relcur_memo">; regular view: <textarea class="relcur_memo">
        memo = div.find(class_="relcur_memo")
        if not memo:
            continue
        texto = memo.get_text(separator="\n", strip=True)
        texto = re.sub(r"\n{3,}", "\n\n", texto).strip()
        if texto:
            textos.append(f"{label}:\n{texto}" if label else texto)
    if textos:
        return "\n\n".join(textos)

    # Abordagem 2: secção após <h3>Pareceres</h3> (fallback para estruturas alternativas)
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        if not re.search(r"^pareceres?$", heading.get_text(strip=True), re.I):
            continue
        heading_level = int(heading.name[1])
        parts: list[str] = []
        for sibling in heading.next_siblings:
            sname = getattr(sibling, "name", None)
            if sname and re.match(r"^h[1-6]$", sname) and int(sname[1]) <= heading_level:
                break
            if hasattr(sibling, "get_text"):
                t = sibling.get_text(separator="\n", strip=True)
                if t:
                    parts.append(t)
            elif isinstance(sibling, str) and sibling.strip():
                parts.append(sibling.strip())
        texto = re.sub(r"\n{3,}", "\n\n", "\n\n".join(parts)).strip()
        if texto:
            return texto

    return None


def obter_pareceres_ano_anterior(
    cur_id: str, ano_atual: str, sessao: SigarraSession
) -> str | None:
    """Obtém os pareceres do relatório do ano letivo anterior (ano N-1).

    Args:
        cur_id:    Identificador do curso.
        ano_atual: Ano letivo atual como string numérica, ex: ``"2024"``.
        sessao:    Sessão autenticada no SIGARRA.

    Returns:
        Texto dos pareceres do ano anterior, ou None se não disponível.
    """
    try:
        ano_int = int(ano_atual)
    except (ValueError, TypeError):
        return None

    relatorios = listar_relatorios_ce(cur_id, sessao=sessao)
    anterior = next((r for r in relatorios if r["ano"] == str(ano_int - 1)), None)
    if anterior is None:
        return None

    pv_id = anterior["pv_id"]
    if pv_id.startswith("3c:"):
        real_id = pv_id[3:]
        url = f"{SIGARRA_BASE}/relcur_geral.rel3c_edit?pv_id={real_id}&pv_print_ver=S"
    else:
        url = SIGARRA_RELCUR_PRINT_URL.format(pv_id)

    try:
        html_str = sessao.fetch_html(url, timeout=30)
    except Exception:
        return None

    return extrair_pareceres_texto(html_str)


# ---------------------------------------------------------------------------
# Submissão de parecer ao SIGARRA
# ---------------------------------------------------------------------------

_SAVE_PARECER_URL = f"{SIGARRA_BASE}/relcur_geral.save_parecer"

_ORGAOS_VALIDOS = {"CC", "CP", "CA"}


def obter_parecer_atual_sigarra(sess: SigarraSession, pv_id: str, orgao: str) -> str:
    """Obtém o texto do parecer atualmente guardado no SIGARRA para um órgão.

    Devolve "" se o campo estiver vazio, se o órgão for inválido, ou em caso de erro.
    """
    orgao_up = (orgao or "").upper()
    if orgao_up not in _ORGAOS_VALIDOS:
        return ""
    if pv_id.startswith("3c:"):
        url = f"{SIGARRA_BASE}/relcur_geral.rel3c_edit?pv_id={pv_id[3:]}"
    else:
        url = f"{SIGARRA_BASE}/relcur_geral.proc_edit?pv_id={pv_id}"
    try:
        html_str = sess.fetch_html(url, timeout=15)
    except Exception:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    campo = f"pv_parecer_{orgao_up.lower()}"
    el = soup.find("textarea", {"name": campo}) or soup.find("textarea", {"id": campo})
    if not el:
        return ""
    return el.get_text(separator="\n", strip=True)


def submeter_parecer_sigarra(
    sess: SigarraSession,
    pv_id: str,
    orgao: str,
    texto: str,
) -> None:
    """Submete um parecer ao SIGARRA via POST.

    Args:
        pv_id:  ID do relatório (pode ser "3c:NNN" para doutoramentos).
        orgao:  "CP", "CC" ou "CA".
        texto:  Texto do parecer (plain text, max 10 000 caracteres).

    Raises:
        ValueError: órgão inválido ou pv_id vazio.
        PermissionError: sessão sem permissão para submeter.
        RuntimeError: erro HTTP inesperado.
    """
    if not pv_id:
        raise ValueError("pv_id não disponível para submissão")
    orgao_up = (orgao or "").upper()
    if orgao_up not in _ORGAOS_VALIDOS:
        raise ValueError(f"Órgão inválido para submissão: {orgao!r} (aceites: CC, CP, CA)")

    inst_id = pv_id[3:] if pv_id.startswith("3c:") else pv_id
    campo = f"pv_parecer_{orgao_up.lower()}"

    data = {
        "pv_inst_id": inst_id,
        "pv_orgao": orgao_up,
        campo: texto[:10000],
    }
    sess.post_form(_SAVE_PARECER_URL, data)


# ---------------------------------------------------------------------------
# Diplomados L que prosseguem para M (FEUP e U.Porto)
# ---------------------------------------------------------------------------

_FEST_LIST_URL = f"{SIGARRA_BASE}/FEST_GERAL.FEST_LIST"
_UP_FEST_URL = "https://sigarra.up.pt/up/pt/u_fest_geral.querylist"

# IDs de todas as unidades orgânicas da U.Porto
_UP_INST_IDS = [
    "62641", "18380", "18395", "18379", "18493", "18383",
    "18487", "18491", "18490", "18381", "18492", "18494",
    "18384", "18489", "18382",
]

_PROSSEGUIMENTO_CACHE: dict[str, tuple[float, dict]] = {}
_PROSSEGUIMENTO_TTL = 24 * 3600  # 24 h


def _pesquisar_estudantes(
    sess: SigarraSession,
    tipo_curso: str,
    estado: int,
    ano_de: str,
    ano_ate: str,
    n_registos: int = 5000,
) -> list[tuple[str, str]]:
    """Pesquisa estudantes na FEUP via FEST_GERAL.FEST_LIST.

    Returns:
        Lista de (codigo_estudante, nome_curso).
    """
    data = {
        "PV_AREA_FORM_CONT_ID": "",
        "PV_NUMERO_DE_ESTUDANTE": "",
        "PV_NOME": "",
        "pv_email": "",
        "pv_tipo_de_curso": tipo_curso,
        "pv_curso_id": "",
        "pv_curso_nome": "",
        "pv_ramo_id": "",
        "pv_ramo_nome": "",
        "PV_ESTADO": str(estado),
        "PV_EM": ano_de,
        "PV_ATE": ano_ate,
        "pv_ano_curr_min": "",
        "pv_ano_curr_max": "",
        "PV_1_INSCRICAO_EM": "",
        "PV_ATE_2": "",
        "PV_TIPO": "",
        "PV_ESTATUTO_ID": "",
        "pv_n_registos": str(n_registos),
        "pv_start": "1",
    }
    html = sess.post_form(_FEST_LIST_URL, data, timeout=300)
    return _parse_fest_list(html)


def _parse_fest_list(html: str) -> list[tuple[str, str]]:
    """Extrai (codigo, curso_nome) da página FEST_GERAL.FEST_LIST (FEUP)."""
    soup = BeautifulSoup(html, "html.parser")
    resultados: list[tuple[str, str]] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        link = tds[0].find("a", href=True)
        if not link or "pct_codigo=" not in link["href"]:
            continue
        codigo = link.get_text(strip=True)
        curso_nome = tds[2].get_text(strip=True)
        resultados.append((codigo, curso_nome))
    return resultados


def _pesquisar_estudantes_up(
    sess: SigarraSession,
    tipo_curso: str,
    estado: int,
    ano: str,
    page_size: int = 5000,
    progress_cb=None,
) -> list[tuple[str, str, str]]:
    """Pesquisa estudantes em todas as faculdades da U.Porto (com paginação).

    Returns:
        Lista de (codigo_estudante, nome_curso, escola).
    """
    resultados: list[tuple[str, str, str]] = []
    pi_inicio = 1
    page = 0

    while True:
        page += 1
        params: list[tuple[str, str]] = [
            ("pv_curso_id", ""),
            ("pv_ramo_id", ""),
        ]
        for inst_id in _UP_INST_IDS:
            params.append(("pa_inst", inst_id))
        params.extend([
            ("PV_NUMERO_DE_ESTUDANTE", ""),
            ("PV_NOME", ""),
            ("PV_EMAIL", ""),
            ("PV_TIPO_DE_CURSO", tipo_curso),
            ("pv_curso_nome", ""),
            ("pv_ramo_nome", ""),
            ("PV_AREA_FORM_CONT_ID", ""),
            ("PV_ESTADO", str(estado)),
            ("PV_EM", ano),
            ("PV_ATE", ""),
            ("PV_1_INSCRICAO_EM", ""),
            ("PV_ATE_2", ""),
            ("PV_TIPO", ""),
            ("pv_n_registos", str(page_size)),
        ])
        if pi_inicio > 1:
            params.append(("pi_inicio", str(pi_inicio)))

        if progress_cb and page > 1:
            progress_cb(f"A obter página {page} de inscritos U.Porto "
                        f"({len(resultados)} registos até agora)...")

        html = sess.post_form(_UP_FEST_URL, params, timeout=300)
        novos = _parse_up_fest_list(html)
        resultados.extend(novos)

        _log.info("prosseguimento UP: página %d → %d registos (total %d)",
                  page, len(novos), len(resultados))

        if len(novos) < page_size:
            break

        pi_inicio += page_size

    return resultados


def _parse_up_fest_list(html: str) -> list[tuple[str, str, str]]:
    """Extrai (codigo, curso_nome, escola) da página u_fest_geral.querylist."""
    soup = BeautifulSoup(html, "html.parser")
    resultados: list[tuple[str, str, str]] = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        link = tds[0].find("a", href=True)
        if not link:
            continue
        codigo = link.get_text(strip=True)
        if not codigo or not codigo[0].isdigit():
            continue
        curso_nome = tds[2].get_text(strip=True)
        escola = tds[3].get_text(strip=True)
        resultados.append((codigo, curso_nome, escola))
    return resultados


def obter_prosseguimento_L_M(
    sess: SigarraSession,
    ano_conclusao: str,
    progress_cb=None,
) -> dict:
    """Calcula percentagem de diplomados L que prosseguem para M (FEUP e U.Porto).

    Args:
        sess:           Sessão SIGARRA autenticada.
        ano_conclusao:  Ano letivo de conclusão (ex: "2023" para 2023/2024).
        progress_cb:    Callback(msg: str) para reportar progresso (opcional).

    Returns:
        Dict com chaves FEUP e U.Porto, por curso de origem e por escola destino.
    """
    cache_key = ano_conclusao
    now = time.time()
    if cache_key in _PROSSEGUIMENTO_CACHE:
        ts, cached = _PROSSEGUIMENTO_CACHE[cache_key]
        if now - ts < _PROSSEGUIMENTO_TTL:
            if progress_cb:
                progress_cb("Prosseguimento L→M obtido de cache")
            return cached

    ano_seguinte = str(int(ano_conclusao) + 1)

    # 1. Diplomados L na FEUP no ano de conclusão
    if progress_cb:
        progress_cb(f"A pesquisar diplomados de licenciatura em {ano_conclusao}/{int(ano_conclusao)+1}...")
    diplomados = _pesquisar_estudantes(sess, "L", 2, ano_conclusao, ano_conclusao)
    _log.info("prosseguimento: %d diplomados L em %s", len(diplomados), ano_conclusao)

    if not diplomados:
        return {}

    codigos_diplomados = {codigo for codigo, _ in diplomados}

    # 2. Inscritos M na FEUP no ano seguinte
    if progress_cb:
        progress_cb(f"A pesquisar inscritos em mestrado na FEUP em {ano_seguinte}/{int(ano_seguinte)+1}...")
    inscritos_m_feup = _pesquisar_estudantes(sess, "M", 1, ano_seguinte, ano_seguinte)
    codigos_m_feup = {codigo for codigo, _ in inscritos_m_feup}
    _log.info("prosseguimento: %d inscritos M FEUP em %s", len(codigos_m_feup), ano_seguinte)

    # 3. Inscritos M em toda a U.Porto no ano seguinte
    inscritos_m_up: list[tuple[str, str, str]] = []
    codigos_m_up: set[str] = set()
    por_escola: dict[str, int] = {}
    try:
        if progress_cb:
            progress_cb(f"A pesquisar inscritos em mestrado na U.Porto em {ano_seguinte}/{int(ano_seguinte)+1}...")
        inscritos_m_up = _pesquisar_estudantes_up(
            sess, "M", 1, ano_seguinte, progress_cb=progress_cb,
        )
        codigos_m_up = {codigo for codigo, _, _ in inscritos_m_up}
        _log.info("prosseguimento: %d inscritos M U.Porto em %s", len(codigos_m_up), ano_seguinte)

        # Distribuição por escola de destino (só diplomados FEUP L que prosseguem)
        for codigo, _, escola in inscritos_m_up:
            if codigo in codigos_diplomados:
                por_escola[escola] = por_escola.get(escola, 0) + 1
    except Exception as e:
        _log.warning("prosseguimento: erro ao pesquisar U.Porto: %s", e)
        if progress_cb:
            progress_cb(f"Pesquisa U.Porto falhou ({e}), usando apenas dados FEUP")

    # 4. Cruzar diplomados com inscritos M
    por_curso: dict[str, dict] = {}
    total_diplomados = 0
    total_feup = 0
    total_up = 0

    for codigo, curso in diplomados:
        if curso not in por_curso:
            por_curso[curso] = {"diplomados": 0, "prosseguem_feup": 0, "prosseguem_up": 0}
        por_curso[curso]["diplomados"] += 1
        total_diplomados += 1
        if codigo in codigos_m_feup:
            por_curso[curso]["prosseguem_feup"] += 1
            total_feup += 1
        if codigos_m_up and codigo in codigos_m_up:
            por_curso[curso]["prosseguem_up"] += 1
            total_up += 1

    for info in por_curso.values():
        d = info["diplomados"]
        info["pct_feup"] = info["prosseguem_feup"] / d * 100 if d > 0 else 0
        info["pct_up"] = info["prosseguem_up"] / d * 100 if d > 0 else 0

    resultado = {
        "total_diplomados_L": total_diplomados,
        "total_prosseguem_M": total_feup,
        "prosseguimento_pct": total_feup / total_diplomados * 100 if total_diplomados > 0 else 0,
        "total_prosseguem_M_up": total_up,
        "prosseguimento_up_pct": total_up / total_diplomados * 100 if total_diplomados > 0 else 0,
        "por_curso": por_curso,
        "por_escola": dict(sorted(por_escola.items(), key=lambda x: -x[1])),
    }

    _PROSSEGUIMENTO_CACHE[cache_key] = (now, resultado)
    return resultado

