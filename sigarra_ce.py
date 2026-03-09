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
    "nome": "", "is_cp": False, "is_cc": False, "cac_cursos": [], "director_cursos": [],
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
    """Obtém cargos de CA de um estudante.

    1. Vai a fest_geral.cursos_list?pv_num_unico=NNN para obter cursos "A Frequentar".
    2. Para cada curso, verifica se o estudante está na CA via
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

    return {**_EMPTY_CARGOS, "nome": nome, "cac_cursos": cac_cursos}


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

