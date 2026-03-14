"""
Extração e agregação de indicadores comparativos de relatórios de CEs.

Extrai indicadores-chave de cada relatório e agrega por nível (L/M/D)
usando rácios de somas (para evitar distorção por cursos pequenos).
"""

from __future__ import annotations

import logging
import re
import time

from bs4 import BeautifulSoup

from sigarra import SigarraSession, SIGARRA_BASE
from sigarra_ce import listar_ces_publicos, listar_relatorios_ce

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache de indicadores agregados por (nivel, ano)
# ---------------------------------------------------------------------------

_AGREGADOS_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
_AGREGADOS_TTL = 24 * 3600  # 24 h

# URLs das versões de impressão
_PRINT_URL_12C = SIGARRA_BASE + "/relcur_geral.proc_edit?pv_id={}&pv_print_ver=S"
_PRINT_URL_3C = SIGARRA_BASE + "/relcur_geral.rel3c_edit?pv_id={}&pv_print_ver=S"


# ---------------------------------------------------------------------------
# Utilitários de parsing
# ---------------------------------------------------------------------------

def _parse_num(s: str) -> float | None:
    """Parse um número de texto SIGARRA (vírgula decimal, %, espaços)."""
    s = s.strip().rstrip("%").strip()
    s = s.replace(",", ".").replace("\xa0", "").replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_count(s: str) -> int | None:
    """Extrai o inteiro inicial de '13 (86.7 %)' ou '13'."""
    m = re.match(r"(\d+)", s.strip())
    return int(m.group(1)) if m else None


def _find_table_after_h3(soup: BeautifulSoup, pattern: str):
    """Encontra a primeira <table> após um <h3> cujo texto corresponda ao padrão."""
    for h3 in soup.find_all("h3"):
        if re.search(pattern, h3.get_text(strip=True), re.I):
            t = h3.find_next("table")
            if t:
                return t
    return None


def _num_data_cols(table) -> int:
    """Conta o número de colunas de dados (excluindo label) pelo cabeçalho <th>."""
    for tr in table.find_all("tr"):
        ths = tr.find_all("th")
        if len(ths) >= 2:
            return len(ths) - 1  # primeira <th> é label vazio
    return 3  # fallback


def _last_col_value(table, row_pattern: str) -> str | None:
    """Valor da última coluna de dados cuja label corresponda ao padrão.

    Usa o nº de colunas do cabeçalho para evitar apanhar células de linhas
    seguintes em tabelas com <tr> mal fechados.
    """
    ncols = _num_data_cols(table)
    for tr in table.find_all("tr"):
        td_k = tr.find("td", class_="k")
        if not td_k:
            continue
        if not re.search(row_pattern, td_k.get_text(strip=True), re.I):
            continue
        # Apenas as primeiras ncols células <td> não-label (ignora nesting)
        tds = [td for td in tr.find_all("td") if td is not td_k][:ncols]
        if not tds:
            continue
        span = tds[-1].find("span", class_="relcur_table")
        return span.get_text(strip=True) if span else tds[-1].get_text(strip=True)
    return None


# ---------------------------------------------------------------------------
# Extração de indicadores de um relatório individual
# ---------------------------------------------------------------------------

def extrair_indicadores(html: str) -> dict | None:
    """Extrai indicadores-chave do HTML bruto de um relatório SIGARRA.

    Devolve dict com valores numéricos brutos (numeradores, denominadores),
    ou None se o parsing falhar por completo.
    """
    soup = BeautifulSoup(html, "html.parser")
    ind: dict = {}

    # --- Procura do CE (última coluna = ano mais recente) ---
    t = _find_table_after_h3(soup, r"Procura do Ciclo")
    if t:
        v = _last_col_value(t, r"N.*vagas")
        if v:
            ind["procura_vagas"] = _parse_count(v)
        v = _last_col_value(t, r"N.*Candidatos")
        if v:
            ind["procura_candidatos"] = _parse_count(v)

    # --- Abandono (última coluna) ---
    t = _find_table_after_h3(soup, r"Abandono do CE")
    if t:
        v = _last_col_value(t, r"N.*de abandonos")
        if v:
            ind["abandono_n"] = _parse_count(v)
        v = _last_col_value(t, r"N.*de inscritos")
        if v:
            ind["abandono_inscritos"] = _parse_count(v)

    # --- Género (Por sexo — última linha = ano mais recente) ---
    t = _find_table_after_h3(soup, r"Por sexo")
    if t:
        rows = t.find_all("tr")
        for row in reversed(rows):
            spans = row.find_all("span", class_="relcur_table")
            if len(spans) >= 3:
                ind["feminino_n"] = _parse_count(spans[1].get_text(strip=True))
                ind["total_estudantes"] = _parse_count(spans[2].get_text(strip=True))
                break

    # --- Nacionalidade (estrangeiros = total - Portugal) ---
    t = _find_table_after_h3(soup, r"Por nacionalidade")
    if t:
        portugal_n = 0
        nat_total = 0
        for row in t.find_all("tr")[1:]:  # saltar cabeçalho
            spans = row.find_all("span", class_="relcur_table")
            if len(spans) >= 2:
                country = spans[0].get_text(strip=True)
                count = _parse_count(spans[1].get_text(strip=True))
                if count is not None:
                    nat_total += count
                    if re.search(r"portugal", country, re.I):
                        portugal_n = count
        if nat_total > 0:
            ind["estrangeiros_n"] = nat_total - portugal_n
            # total_estudantes_nac para cross-check (pode diferir de "Por sexo" se inclui mobilidade)
            if "total_estudantes" not in ind:
                ind["total_estudantes"] = nat_total

    # --- Estatística docente (linha "Contrato") ---
    t = _find_table_after_h3(soup, r"Estat.stica docente")
    if t:
        for tr in t.find_all("tr"):
            spans = tr.find_all("span", class_="relcur_table")
            if spans and re.search(r"contrato", spans[0].get_text(strip=True), re.I):
                vals = [s.get_text(strip=True) for s in spans[1:]]
                # Colunas: Total_N, Total_ETI, TI_N, TI_ETI, TI_%,
                #          Dout_N, Dout_ETI, Dout_%, TI+Dout_N, TI+Dout_ETI, TI+Dout_%
                if len(vals) >= 8:
                    ind["docentes_total_eti"] = _parse_num(vals[1])
                    ind["docentes_doutorados_eti"] = _parse_num(vals[6])
                break

    # --- Carreira docente (integrados na carreira) ---
    # Colunas: Integrados_Nº, Integrados_%, Catedráticos_Nº, Catedráticos_%, ...
    t = _find_table_after_h3(soup, r"Carreira docente")
    if t:
        for row in t.find_all("tr"):
            spans = row.find_all("span", class_="relcur_table")
            if len(spans) >= 2:
                integrados_eti = _parse_num(spans[0].get_text(strip=True))
                integrados_pct = _parse_num(spans[1].get_text(strip=True))
                if integrados_eti is not None:
                    ind["docentes_integrados_eti"] = integrados_eti
                    # Derivar total_eti quando "Estatística docente" não existe (3º ciclo)
                    if "docentes_total_eti" not in ind and integrados_pct and integrados_pct > 0:
                        ind["docentes_total_eti"] = integrados_eti / (integrados_pct / 100)
                break

    # --- Eficiência formativa (última coluna) ---
    t = _find_table_after_h3(soup, r"Efici.ncia formativa")
    if t:
        v = _last_col_value(t, r"N.*diplomados$")
        if v:
            ind["diplomados_total"] = _parse_count(v)
        # Primeira linha "Nº diplomados em N anos" = duração nominal
        v = _last_col_value(t, r"N.*diplomados em \d")
        if v:
            ind["diplomados_no_tempo"] = _parse_count(v)
        v = _last_col_value(t, r"Classifica..o m.dia")
        if v:
            ind["classif_media_saida"] = _parse_num(v)

    # --- Aprovação 1º ano, 1ª vez (primeira tabela após h3) ---
    for h3 in soup.find_all("h3"):
        if re.search(r"aprova..o.*1.*ano", h3.get_text(strip=True), re.I):
            table = h3.find_next("table")
            if table:
                v = _last_col_value(table, r"Total de inscritos.*1.*vez")
                if v:
                    ind["aprovacao_1ano_inscritos"] = _parse_count(v)
                v = _last_col_value(table, r">=75%")
                if v:
                    ind["aprovacao_1ano_75pct"] = _parse_count(v)
            break

    # --- Inquéritos Pedagógicos (IPUP) ---
    t = _find_table_after_h3(soup, r"Inqu.ritos Pedag.gicos")
    if t:
        medianas: list[float] = []
        taxa_vals: list[float] = []
        for tr in t.find_all("tr"):
            td_k = tr.find("td", class_="k")
            if not td_k:
                continue
            label = td_k.get_text(strip=True)
            spans = [s for s in tr.find_all("span", class_="relcur_table")]
            if re.search(r"Taxa de preenchimento", label, re.I):
                for s in spans:
                    v = _parse_num(s.get_text(strip=True))
                    if v is not None:
                        taxa_vals.append(v)
            else:
                for s in spans:
                    v = _parse_num(s.get_text(strip=True))
                    if v is not None:
                        medianas.append(v)
        if medianas:
            ind["ipup_mediana_global"] = sum(medianas) / len(medianas)
        if taxa_vals:
            ind["ipup_taxa_preenchimento"] = sum(taxa_vals) / len(taxa_vals)

    # --- Preenchimento de sumários ---
    t = _find_table_after_h3(soup, r"Preenchimento dos sum.rios")
    if t:
        last_vals: list[float] = []
        for tr in t.find_all("tr"):
            spans = tr.find_all("span", class_="relcur_table")
            if not spans:
                continue
            # Último valor não-N/A da linha (última data de cada semestre)
            for s in reversed(spans):
                v = _parse_num(s.get_text(strip=True))
                if v is not None:
                    last_vals.append(v)
                    break
        if last_vals:
            ind["sumarios_pct"] = sum(last_vals) / len(last_vals)

    # --- Empregabilidade (campos input) ---
    inp = soup.find("input", attrs={"name": "DIPLOM_AREAS_CE"})
    if inp and inp.get("value", "").strip():
        v = _parse_num(inp["value"])
        if v is not None:
            ind["empregabilidade_area_pct"] = v

    return ind if ind else None


# ---------------------------------------------------------------------------
# Agregação de indicadores (rácios de somas / médias pesadas)
# ---------------------------------------------------------------------------

_SOMA_KEYS = [
    "procura_candidatos", "procura_vagas",
    "abandono_n", "abandono_inscritos",
    "feminino_n", "total_estudantes", "estrangeiros_n",
    "docentes_total_eti", "docentes_doutorados_eti", "docentes_integrados_eti",
    "diplomados_total", "diplomados_no_tempo",
    "aprovacao_1ano_inscritos", "aprovacao_1ano_75pct",
]


def _agregar_indicadores(lista: list[dict]) -> dict:
    """Agrega indicadores de vários cursos usando rácios de somas."""
    somas: dict[str, float] = {k: 0.0 for k in _SOMA_KEYS}
    # Médias pesadas
    classif_sum = 0.0
    classif_w = 0
    ipup_sum = 0.0
    ipup_w = 0
    ipup_taxa_sum = 0.0
    ipup_taxa_w = 0
    sumarios_sum = 0.0
    sumarios_n = 0
    empreg_sum = 0.0
    empreg_n = 0

    for ind in lista:
        for k in _SOMA_KEYS:
            v = ind.get(k)
            if v is not None:
                somas[k] += v
        # Classificação pesada por nº diplomados
        d = ind.get("diplomados_total") or 0
        if ind.get("classif_media_saida") and d > 0:
            classif_sum += ind["classif_media_saida"] * d
            classif_w += d
        # IPUP pesada por nº estudantes
        n_est = ind.get("total_estudantes") or 0
        if ind.get("ipup_mediana_global") and n_est > 0:
            ipup_sum += ind["ipup_mediana_global"] * n_est
            ipup_w += n_est
        if ind.get("ipup_taxa_preenchimento") and n_est > 0:
            ipup_taxa_sum += ind["ipup_taxa_preenchimento"] * n_est
            ipup_taxa_w += n_est
        # Sumários — média simples entre cursos
        if ind.get("sumarios_pct") is not None:
            sumarios_sum += ind["sumarios_pct"]
            sumarios_n += 1
        # Empregabilidade — média simples (dados manuais, sem denominador claro)
        if ind.get("empregabilidade_area_pct") is not None:
            empreg_sum += ind["empregabilidade_area_pct"]
            empreg_n += 1

    def _ratio(num_k: str, den_k: str) -> float | None:
        d = somas[den_k]
        return somas[num_k] / d * 100 if d > 0 else None

    # Abandono: abandonos / (abandonos + inscritos)
    ab_total = somas["abandono_n"] + somas["abandono_inscritos"]
    abandono_pct = somas["abandono_n"] / ab_total * 100 if ab_total > 0 else None

    return {
        "n_cursos": len(lista),
        "total_estudantes": int(somas["total_estudantes"]),
        "procura_ratio": (somas["procura_candidatos"] / somas["procura_vagas"]
                          if somas["procura_vagas"] > 0 else None),
        "abandono_pct": abandono_pct,
        "feminino_pct": _ratio("feminino_n", "total_estudantes"),
        "estrangeiros_pct": _ratio("estrangeiros_n", "total_estudantes"),
        "docentes_doutorados_pct": (somas["docentes_doutorados_eti"] / somas["docentes_total_eti"] * 100
                                    if somas["docentes_total_eti"] > 0 else None),
        "docentes_integrados_pct": (somas["docentes_integrados_eti"] / somas["docentes_total_eti"] * 100
                                    if somas["docentes_total_eti"] > 0 else None),
        "eficiencia_formativa_pct": _ratio("diplomados_no_tempo", "diplomados_total"),
        "classif_media_saida": classif_sum / classif_w if classif_w > 0 else None,
        "aprovacao_1ano_75pct": _ratio("aprovacao_1ano_75pct", "aprovacao_1ano_inscritos"),
        "ipup_mediana_global": ipup_sum / ipup_w if ipup_w > 0 else None,
        "ipup_taxa_preenchimento": ipup_taxa_sum / ipup_taxa_w if ipup_taxa_w > 0 else None,
        "sumarios_pct": sumarios_sum / sumarios_n if sumarios_n > 0 else None,
        "empregabilidade_area_pct": empreg_sum / empreg_n if empreg_n > 0 else None,
    }


# ---------------------------------------------------------------------------
# Obtenção e agregação de indicadores para um nível e ano letivo
# ---------------------------------------------------------------------------

def obter_indicadores_agregados(
    sess: SigarraSession,
    nivel: str,
    ano: str,
    progress_cb=None,
) -> dict | None:
    """Obtém indicadores agregados de todos os CEs do mesmo nível/ano.

    Args:
        sess:        Sessão SIGARRA autenticada.
        nivel:       Tipo de CE: "L", "M" ou "D".
        ano:         Ano letivo (ex: "2024").
        progress_cb: Callback(msg: str) para reportar progresso (opcional).

    Returns:
        Dict com indicadores agregados ou None se dados insuficientes.
        Resultado em cache durante 24h.
    """
    cache_key = (nivel.upper(), ano)
    now = time.time()
    if cache_key in _AGREGADOS_CACHE:
        ts, cached = _AGREGADOS_CACHE[cache_key]
        if now - ts < _AGREGADOS_TTL:
            if progress_cb:
                progress_cb(f"Indicadores comparativos ({nivel}) obtidos de cache")
            return cached

    # Listar CEs do mesmo nível
    ces = [c for c in listar_ces_publicos() if c["tipo"] == nivel.upper()]
    if not ces:
        return None

    if progress_cb:
        progress_cb(f"A extrair indicadores de {len(ces)} {_NIVEL_LABEL.get(nivel.upper(), 'cursos')}...")

    indicadores_list: list[dict] = []
    erros = 0

    for i, ce in enumerate(ces):
        cur_id = ce["cur_id"]
        # Obter pv_id do relatório para o ano solicitado
        relatorios = listar_relatorios_ce(cur_id, sess)
        rel = next((r for r in relatorios if r["ano"] == ano), None)
        if not rel:
            continue

        pv_id = rel["pv_id"]
        if pv_id.startswith("3c:"):
            url = _PRINT_URL_3C.format(pv_id[3:])
        else:
            url = _PRINT_URL_12C.format(pv_id)

        try:
            html = sess.fetch_html(url, timeout=30)
            ind = extrair_indicadores(html)
            if ind:
                indicadores_list.append(ind)
        except Exception as e:
            erros += 1
            _log.warning("indicadores: erro ao processar %s (cur_id=%s): %s",
                         ce.get("nome", "?"), cur_id, e)

        if progress_cb and (i + 1) % 5 == 0:
            progress_cb(f"Indicadores: {i + 1}/{len(ces)} cursos processados...")

    if len(indicadores_list) < 3:
        _log.warning("indicadores: apenas %d cursos com dados para %s/%s",
                     len(indicadores_list), nivel, ano)
        return None

    resultado = _agregar_indicadores(indicadores_list)
    if progress_cb:
        ok = len(indicadores_list)
        progress_cb(f"Indicadores comparativos: {ok} cursos agregados"
                    + (f" ({erros} com erros)" if erros else ""))

    _AGREGADOS_CACHE[cache_key] = (now, resultado)
    return resultado


_NIVEL_LABEL = {"L": "licenciaturas", "M": "mestrados", "D": "doutoramentos"}


# ---------------------------------------------------------------------------
# Formatação para inclusão na prompt do LLM
# ---------------------------------------------------------------------------

def formatar_indicadores_prompt(agregados: dict, nivel: str) -> str:
    """Formata indicadores agregados como secção de texto para a prompt do LLM."""
    nivel_label = _NIVEL_LABEL.get(nivel.upper(), "cursos")
    n = agregados.get("n_cursos", 0)
    total_est = agregados.get("total_estudantes", 0)

    linhas = [
        f"## Indicadores comparativos ({nivel_label} FEUP, N={n} cursos, {total_est} estudantes)",
        "",
        "Valores agregados de todos os cursos do mesmo nível neste ano letivo,",
        "calculados como rácios de somas (pesados pela dimensão de cada curso).",
        "Usar como referência para contextualizar os indicadores do curso em análise.",
        "",
    ]

    def _fmt(label: str, key: str, suffix: str = "%", decimals: int = 1):
        v = agregados.get(key)
        if v is not None:
            linhas.append(f"- {label}: {v:.{decimals}f}{suffix}")

    _fmt("Rácio candidatos/vagas", "procura_ratio", "x")
    _fmt("Taxa de abandono", "abandono_pct")
    _fmt("Género feminino", "feminino_pct")
    _fmt("Estudantes estrangeiros", "estrangeiros_pct")
    _fmt("Docentes doutorados (ETI)", "docentes_doutorados_pct")
    _fmt("Docentes integrados na carreira (ETI)", "docentes_integrados_pct")
    _fmt("Eficiência formativa (diplomados no tempo previsto)", "eficiencia_formativa_pct")
    _fmt("Classificação média de saída", "classif_media_saida", " valores", 1)
    _fmt("Aprovação 1º ano 1ª vez (>=75% ECTS)", "aprovacao_1ano_75pct")
    _fmt("Mediana global IPUP (escala 1-7)", "ipup_mediana_global", "", 2)
    _fmt("Taxa de preenchimento IPUP", "ipup_taxa_preenchimento")
    _fmt("Preenchimento de sumários", "sumarios_pct")
    _fmt("Empregabilidade na área do CE", "empregabilidade_area_pct")

    return "\n".join(linhas)
