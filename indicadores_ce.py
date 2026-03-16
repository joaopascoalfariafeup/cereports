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
PRINT_URL_12C = SIGARRA_BASE + "/relcur_geral.proc_edit?pv_id={}&pv_print_ver=S"
PRINT_URL_3C = SIGARRA_BASE + "/relcur_geral.rel3c_edit?pv_id={}&pv_print_ver=S"


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
        v = _last_col_value(t, r"N.*Colocados$")
        if v:
            ind["procura_colocados"] = _parse_count(v)
        v = _last_col_value(t, r"Nota m.+dia de entrada")
        if v:
            ind["nota_media_entrada"] = _parse_num(v)
        _log.debug("indicadores procura: vagas=%s cand=%s coloc=%s nota=%s",
                   ind.get("procura_vagas"), ind.get("procura_candidatos"),
                   ind.get("procura_colocados"), ind.get("nota_media_entrada"))

    # --- Abandono (última coluna) ---
    t = _find_table_after_h3(soup, r"Abandono do CE")
    if t:
        v = _last_col_value(t, r"N.*de abandonos")
        if v:
            ind["abandono_n"] = _parse_count(v)
        v = _last_col_value(t, r"N.*de inscritos")
        if v:
            ind["abandono_inscritos"] = _parse_count(v)

    # --- Sexo (Por sexo — última linha = ano mais recente) ---
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
    t = _find_table_after_h3(soup, r"Estat.+stica docente")
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

    # --- Carreira docente (integrados na carreira + investigação) ---
    # Colunas: Integrados_Nº, Integrados_%, Catedráticos_Nº, Catedráticos_%,
    #          Investigação_Nº, Investigação_%
    t = _find_table_after_h3(soup, r"Carreira docente")
    if t:
        for row in t.find_all("tr"):
            spans = row.find_all("span", class_="relcur_table")
            if len(spans) >= 2:
                integrados_eti = _parse_num(spans[0].get_text(strip=True))
                integrados_pct = _parse_num(spans[1].get_text(strip=True))
                if integrados_eti is not None:
                    ind["docentes_integrados_eti"] = integrados_eti
                    # Total para carreira = internos+externos (derivado da % do SIGARRA)
                    if integrados_pct and integrados_pct > 0:
                        ind["docentes_total_eti_carreira"] = integrados_eti / (integrados_pct / 100)
                    # Derivar total_eti internos quando "Estatística docente" não existe (3º ciclo)
                    if "docentes_total_eti" not in ind and integrados_pct and integrados_pct > 0:
                        ind["docentes_total_eti"] = integrados_eti / (integrados_pct / 100)
                # Docentes em unidades de investigação (colunas 5-6, só doutoramentos)
                if len(spans) >= 5:
                    inv_eti = _parse_num(spans[4].get_text(strip=True))
                    if inv_eti is not None:
                        ind["docentes_investigacao_eti"] = inv_eti
                break

    # --- Corpo docente (interno + externo): doutorados e esforço integrados ---
    # h4 "Corpo docente - interno/externo"; colunas (spans):
    #   0:Nome, 1:UO, 2:Categoria, 3:Grau, 4:Área do grau,
    #   5:Regime de tempo (ETI), 6:Esforço CE (ETI), 7:Esforço sem OT, 8:Link
    # Linha "Totais": td[0]=Totais, td[1]=count(colspan=4),
    #   td[2]=Regime total, td[3]=Esforço CE total, td[4]=Esforço sem OT total
    total_esforco_integrados = 0.0  # esforço de não-convidados com Regime=1.0
    total_esforco_ce = 0.0          # esforço total de todos os docentes
    ext_dout_eti = 0.0              # ETI Regime de doutorados externos

    for h4 in soup.find_all("h4"):
        if not re.search(r"Corpo docente", h4.get_text(strip=True), re.I):
            continue
        is_externo = bool(re.search(r"externo", h4.get_text(strip=True), re.I))
        t_cd = h4.find_next("table")
        if not t_cd:
            continue
        for tr in t_cd.find_all("tr"):
            # Linha "Totais"
            td_k = tr.find("td", class_="k")
            if td_k and re.search(r"^Totais$", td_k.get_text(strip=True), re.I):
                tds = tr.find_all("td")
                # td[3] = Esforço Docente no CE total (após td Totais + td colspan=4 + td Regime)
                if len(tds) >= 4:
                    v = _parse_num(tds[3].get_text(strip=True))
                    if v is not None:
                        total_esforco_ce += v
                continue
            # Linhas individuais (com spans)
            spans = tr.find_all("span", class_="relcur_table")
            if len(spans) < 7:
                continue
            categoria = spans[2].get_text(strip=True)
            grau = spans[3].get_text(strip=True)
            regime_eti = _parse_num(spans[5].get_text(strip=True))
            esforco_ce = _parse_num(spans[6].get_text(strip=True)) or 0.0

            # Doutorados externos (Regime de tempo)
            if is_externo and re.search(r"doutoramento", grau, re.I):
                if regime_eti is not None:
                    ext_dout_eti += regime_eti

            # Integrados por esforço: não-Convidado + Regime=1.0
            if not re.search(r"convidado", categoria, re.I) and regime_eti == 1.0:
                total_esforco_integrados += esforco_ce

    if ext_dout_eti > 0:
        ind["docentes_doutorados_ext_eti"] = ext_dout_eti
    if total_esforco_ce > 0:
        ind["esforco_integrados_eti"] = total_esforco_integrados
        ind["esforco_total_eti"] = total_esforco_ce

    # --- Eficiência formativa (última coluna) ---
    t = _find_table_after_h3(soup, r"Efici.+ncia formativa")
    if t:
        v = _last_col_value(t, r"N.*diplomados$")
        if v:
            ind["diplomados_total"] = _parse_count(v)
        # Primeira linha "Nº diplomados em N anos" = duração nominal
        v = _last_col_value(t, r"N.*diplomados em \d")
        if v:
            ind["diplomados_no_tempo"] = _parse_count(v)
        v = _last_col_value(t, r"Classifica.+o m.+dia")
        if v:
            ind["classif_media_saida"] = _parse_num(v)

    # --- Aprovação 1º ano, 1ª vez (primeira tabela após h3) ---
    for h3 in soup.find_all("h3"):
        if re.search(r"aprova.+o.*1.*ano", h3.get_text(strip=True), re.I):
            table = h3.find_next("table")
            if table:
                v = _last_col_value(table, r"Total de inscritos.*1.*vez")
                if v:
                    ind["aprovacao_1ano_inscritos"] = _parse_count(v)
                v = _last_col_value(table, r">=75%")
                if v:
                    ind["aprovacao_1ano_75pct"] = _parse_count(v)
            break

    # --- Estudantes por ano curricular (última linha = ano mais recente) ---
    # Usado como peso para a média pesada da taxa IPUP
    estudantes_por_ano: list[int] = []
    t = _find_table_after_h3(soup, r"Estudantes por ano curricular")
    if t:
        rows = t.find_all("tr")
        for row in reversed(rows):
            spans = row.find_all("span", class_="relcur_table")
            if len(spans) >= 2:
                # Últimos spans menos o último (que é "Número total")
                for s in spans[:-1]:
                    c = _parse_count(s.get_text(strip=True))
                    if c is not None:
                        estudantes_por_ano.append(c)
                break

    # --- Inquéritos Pedagógicos (IPUP) ---
    t = _find_table_after_h3(soup, r"Inqu.+ritos Pedag.+gicos")
    if t:
        # medianas_por_ano[i] = lista de medianas (dimensões) do ano curricular i
        medianas_por_ano: dict[int, list[float]] = {}
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
                for i, s in enumerate(spans):
                    v = _parse_num(s.get_text(strip=True))
                    if v is not None:
                        medianas_por_ano.setdefault(i, []).append(v)

        # Média da mediana por ano curricular
        media_por_ano = {i: sum(vs) / len(vs) for i, vs in medianas_por_ano.items() if vs}
        n_anos = len(media_por_ano)

        # Pesos = nº estimado de respostas por ano (taxa% * nº estudantes / 100)
        pode_pesar = (estudantes_por_ano
                      and len(estudantes_por_ano) >= n_anos
                      and len(taxa_vals) >= n_anos)
        if pode_pesar:
            respostas_por_ano = [taxa_vals[i] * estudantes_por_ano[i] / 100
                                 for i in range(n_anos)]
            total_respostas = sum(respostas_por_ano)
        else:
            respostas_por_ano = []
            total_respostas = 0

        if media_por_ano:
            if total_respostas > 0:
                ind["ipup_mediana_global"] = sum(
                    media_por_ano[i] * respostas_por_ano[i] for i in range(n_anos)
                ) / total_respostas
            else:
                # Fallback: média simples dos anos
                ind["ipup_mediana_global"] = sum(media_por_ano.values()) / n_anos

        if taxa_vals:
            if total_respostas > 0:
                ind["ipup_taxa_preenchimento"] = total_respostas / sum(estudantes_por_ano[:n_anos]) * 100
            else:
                ind["ipup_taxa_preenchimento"] = sum(taxa_vals) / len(taxa_vals)

        # Nº estimado de respostas IPUP (peso para agregação entre cursos)
        if total_respostas > 0:
            ind["ipup_respostas_est"] = total_respostas

    # --- Preenchimento de sumários ---
    t = _find_table_after_h3(soup, r"Preenchimento dos sum.+rios")
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

    # --- Teses defendidas — nº médio de anos para conclusão (doutoramentos) ---
    t = _find_table_after_h3(soup, r"Teses defendidas")
    if t:
        anos_conclusao: list[float] = []
        for tr in t.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 4:
                # Última coluna: "Número de anos para conclusão"
                v = _parse_num(tds[-1].get_text(strip=True))
                if v is not None and v > 0:
                    anos_conclusao.append(v)
        if anos_conclusao:
            ind["teses_n"] = len(anos_conclusao)
            ind["teses_soma_anos"] = sum(anos_conclusao)

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
    "procura_candidatos", "procura_vagas", "procura_colocados",
    "abandono_n", "abandono_inscritos",
    "feminino_n", "total_estudantes", "estrangeiros_n",
    "docentes_total_eti", "docentes_total_eti_carreira",
    "docentes_doutorados_eti", "docentes_doutorados_ext_eti",
    "docentes_integrados_eti",
    "docentes_investigacao_eti",
    "esforco_integrados_eti", "esforco_total_eti",
    "diplomados_total", "diplomados_no_tempo",
    "teses_n", "teses_soma_anos",
    "aprovacao_1ano_inscritos", "aprovacao_1ano_75pct",
]


def _agregar_indicadores(lista: list[dict]) -> dict:
    """Agrega indicadores de vários cursos usando rácios de somas."""
    somas: dict[str, float] = {k: 0.0 for k in _SOMA_KEYS}
    contagens: dict[str, int] = {k: 0 for k in _SOMA_KEYS}  # cursos que contribuíram
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
    nota_sum = 0.0
    nota_w = 0

    for ind in lista:
        for k in _SOMA_KEYS:
            v = ind.get(k)
            if v is not None:
                somas[k] += v
                contagens[k] += 1
        # Classificação pesada por nº diplomados
        d = ind.get("diplomados_total") or 0
        if ind.get("classif_media_saida") and d > 0:
            classif_sum += ind["classif_media_saida"] * d
            classif_w += d
        # IPUP pesada por nº estimado de respostas (fallback: nº estudantes)
        n_resp = ind.get("ipup_respostas_est") or ind.get("total_estudantes") or 0
        if ind.get("ipup_mediana_global") and n_resp > 0:
            ipup_sum += ind["ipup_mediana_global"] * n_resp
            ipup_w += n_resp
        if ind.get("ipup_taxa_preenchimento") and n_resp > 0:
            ipup_taxa_sum += ind["ipup_taxa_preenchimento"] * n_resp
            ipup_taxa_w += n_resp
        # Sumários — média simples entre cursos
        if ind.get("sumarios_pct") is not None:
            sumarios_sum += ind["sumarios_pct"]
            sumarios_n += 1
        # Empregabilidade — média simples (dados manuais, sem denominador claro)
        if ind.get("empregabilidade_area_pct") is not None:
            empreg_sum += ind["empregabilidade_area_pct"]
            empreg_n += 1
        # Nota média de entrada pesada por nº colocados
        n_col = ind.get("procura_colocados") or 0
        if ind.get("nota_media_entrada") and n_col > 0:
            nota_sum += ind["nota_media_entrada"] * n_col
            nota_w += n_col

    def _ratio(num_k: str, den_k: str) -> float | None:
        # Só calcula se pelo menos 1 curso contribuiu para o numerador
        if contagens[num_k] == 0:
            return None
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
        "nota_media_entrada": nota_sum / nota_w if nota_w > 0 else None,
        "abandono_pct": abandono_pct,
        "feminino_pct": _ratio("feminino_n", "total_estudantes"),
        "estrangeiros_pct": _ratio("estrangeiros_n", "total_estudantes"),
        "docentes_doutorados_pct": ((somas["docentes_doutorados_eti"] + somas["docentes_doutorados_ext_eti"])
                                    / somas["docentes_total_eti_carreira"] * 100
                                    if contagens["docentes_doutorados_eti"] > 0 and somas["docentes_total_eti_carreira"] > 0
                                    else None),
        "docentes_integrados_pct": (somas["docentes_integrados_eti"] / somas["docentes_total_eti_carreira"] * 100
                                    if contagens["docentes_integrados_eti"] > 0 and somas["docentes_total_eti_carreira"] > 0
                                    else None),
        "docentes_investigacao_pct": (somas["docentes_investigacao_eti"] / somas["docentes_total_eti_carreira"] * 100
                                      if contagens["docentes_investigacao_eti"] > 0 and somas["docentes_total_eti_carreira"] > 0
                                      else None),
        "esforco_integrados_pct": _ratio("esforco_integrados_eti", "esforco_total_eti"),
        "estudantes_por_docente_eti": (somas["total_estudantes"] / somas["esforco_total_eti"]
                                       if somas["esforco_total_eti"] > 0 else None),
        "eficiencia_formativa_pct": _ratio("diplomados_no_tempo", "diplomados_total"),
        "teses_media_anos": (somas["teses_soma_anos"] / somas["teses_n"]
                             if somas["teses_n"] > 0 else None),
        "teses_n": int(somas["teses_n"]) if somas["teses_n"] > 0 else None,
        "classif_media_saida": classif_sum / classif_w if classif_w > 0 else None,
        "aprovacao_1ano_75pct": _ratio("aprovacao_1ano_75pct", "aprovacao_1ano_inscritos"),
        "ipup_mediana_global": ipup_sum / ipup_w if ipup_w > 0 else None,
        "ipup_taxa_preenchimento": ipup_taxa_sum / ipup_taxa_w if ipup_taxa_w > 0 else None,
        "sumarios_pct": sumarios_sum / sumarios_n if sumarios_n > 0 else None,
        "empregabilidade_area_pct": empreg_sum / empreg_n if empreg_n >= 3 else None,
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
            url = PRINT_URL_3C.format(pv_id[3:])
        else:
            url = PRINT_URL_12C.format(pv_id)

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
# Rácios de um CE individual (mesmas chaves que a agregação)
# ---------------------------------------------------------------------------

def calcular_racios(ind: dict) -> dict:
    """Calcula rácios a partir dos indicadores brutos de um CE individual.

    Devolve dict com as mesmas chaves que _agregar_indicadores, permitindo
    comparação directa entre CE e agregado.
    """
    def _safe_div(num, den, scale=100):
        return num / den * scale if num is not None and den and den > 0 else None

    total_est = ind.get("total_estudantes")
    total_eti = ind.get("docentes_total_eti")
    total_eti_carreira = ind.get("docentes_total_eti_carreira") or total_eti

    ab_n = ind.get("abandono_n")
    ab_ins = ind.get("abandono_inscritos")
    ab_total = (ab_n or 0) + (ab_ins or 0)

    r: dict = {}
    r["procura_ratio"] = _safe_div(ind.get("procura_candidatos"),
                                    ind.get("procura_vagas"), 1)
    _log.debug("calcular_racios procura: cand=%s vagas=%s ratio=%s",
               ind.get("procura_candidatos"), ind.get("procura_vagas"), r["procura_ratio"])
    r["nota_media_entrada"] = ind.get("nota_media_entrada")
    r["abandono_pct"] = _safe_div(ab_n, ab_total) if ab_n is not None else None
    r["feminino_pct"] = _safe_div(ind.get("feminino_n"), total_est)
    r["estrangeiros_pct"] = _safe_div(ind.get("estrangeiros_n"), total_est)
    dout_total = (ind.get("docentes_doutorados_eti") or 0) + (ind.get("docentes_doutorados_ext_eti") or 0)
    r["docentes_doutorados_pct"] = _safe_div(dout_total, total_eti_carreira) if dout_total > 0 else None
    r["docentes_integrados_pct"] = _safe_div(ind.get("docentes_integrados_eti"), total_eti_carreira)
    r["docentes_investigacao_pct"] = _safe_div(ind.get("docentes_investigacao_eti"), total_eti_carreira)
    r["esforco_integrados_pct"] = _safe_div(ind.get("esforco_integrados_eti"),
                                             ind.get("esforco_total_eti"))
    esf_total = ind.get("esforco_total_eti")
    r["estudantes_por_docente_eti"] = (total_est / esf_total
                                       if total_est and esf_total and esf_total > 0
                                       else None)
    r["eficiencia_formativa_pct"] = _safe_div(ind.get("diplomados_no_tempo"),
                                               ind.get("diplomados_total"))
    r["classif_media_saida"] = ind.get("classif_media_saida")
    r["aprovacao_1ano_75pct"] = _safe_div(ind.get("aprovacao_1ano_75pct"),
                                           ind.get("aprovacao_1ano_inscritos"))
    r["ipup_mediana_global"] = ind.get("ipup_mediana_global")
    r["ipup_taxa_preenchimento"] = ind.get("ipup_taxa_preenchimento")
    r["sumarios_pct"] = ind.get("sumarios_pct")
    r["empregabilidade_area_pct"] = ind.get("empregabilidade_area_pct")

    # Teses (doutoramentos)
    teses_n = ind.get("teses_n")
    teses_soma = ind.get("teses_soma_anos")
    r["teses_media_anos"] = teses_soma / teses_n if teses_n and teses_soma else None
    r["teses_n"] = teses_n

    return r


# ---------------------------------------------------------------------------
# Formatação para inclusão na prompt do LLM
# ---------------------------------------------------------------------------

def formatar_indicadores_prompt(agregados: dict, nivel: str,
                                ce_individual: dict | None = None) -> str:
    """Formata indicadores agregados como secção de texto para a prompt do LLM.

    Se ce_individual for fornecido (indicadores brutos do CE em análise),
    mostra o valor do CE entre parêntesis ao lado do agregado.
    """
    nivel_label = _NIVEL_LABEL.get(nivel.upper(), "cursos")
    n = agregados.get("n_cursos", 0)
    total_est = agregados.get("total_estudantes", 0)

    # Calcular rácios do CE individual (se fornecido)
    ce_r = calcular_racios(ce_individual) if ce_individual else {}

    linhas = [
        f"## Indicadores comparativos ({nivel_label} FEUP, N={n} cursos, {total_est} estudantes)",
        "",
        "Valores agregados de todos os cursos do mesmo nível neste ano letivo,",
        "calculados como rácios de somas (pesados pela dimensão de cada curso).",
        "Entre parêntesis, o valor do ciclo de estudos em análise."
        if ce_individual else
        "Usar como referência para contextualizar os indicadores do curso em análise.",
        "",
    ]

    def _fmt(label: str, key: str, suffix: str = "%", decimals: int = 1):
        v = agregados.get(key)
        if v is not None:
            ce_v = ce_r.get(key)
            ce_txt = (f" (CE: {ce_v:.{decimals}f}{suffix})" if ce_v is not None
                      else " (CE: N/A)" if ce_individual else "")
            linhas.append(f"- {label}: {v:.{decimals}f}{suffix}{ce_txt}")

    # -- Procura e atratividade --
    linhas.append("### Procura e atratividade")
    _procura_label = ("Rácio candidatos 1ª opção/vagas"
                       if nivel.upper() == "L" else "Rácio candidatos/vagas")
    _fmt(_procura_label, "procura_ratio", "x", 2)
    if nivel.upper() == "L":
        _fmt("Nota média de entrada (pesada por nº colocados)", "nota_media_entrada", " pontos")

    # -- Caracterização dos estudantes --
    linhas.append("### Caracterização dos estudantes")
    _fmt("Sexo feminino", "feminino_pct")
    _fmt("Estudantes estrangeiros", "estrangeiros_pct")

    # -- Recursos docentes --
    linhas.append("### Recursos docentes")
    _fmt("Docentes doutorados (ETI, contrato)", "docentes_doutorados_pct")
    _fmt("Docentes integrados na carreira (ETI, contrato)", "docentes_integrados_pct")
    _fmt("Docentes em unidades de investigação (ETI)", "docentes_investigacao_pct")
    _fmt("Esforço docente assegurado por docentes integrados na carreira (ETI)", "esforco_integrados_pct")
    _fmt("Estudantes inscritos por docente (ETI esforço)", "estudantes_por_docente_eti", "", 1)

    # -- Processos internos --
    linhas.append("### Processos internos")
    _fmt("Preenchimento de sumários (última data disponível)", "sumarios_pct")

    # -- Resultados: satisfação dos estudantes --
    linhas.append("### Resultados: satisfação dos estudantes")
    _fmt("IPUP média das medianas (escala 1-7, pesada por nº estudantes)", "ipup_mediana_global", "", 2)
    _fmt("Taxa de preenchimento IPUP", "ipup_taxa_preenchimento")

    # -- Resultados: sucesso escolar --
    linhas.append("### Resultados: sucesso escolar")
    _fmt("Taxa de abandono", "abandono_pct")
    _fmt("Aprovação 1º ano 1ª vez (>=75% ECTS)", "aprovacao_1ano_75pct")
    _fmt("Eficiência formativa (diplomados no tempo mínimo previsto)", "eficiencia_formativa_pct")
    _fmt("Classificação média de saída", "classif_media_saida", " valores", 1)
    # Teses — média de anos para conclusão (só doutoramentos)
    teses_media = agregados.get("teses_media_anos")
    teses_total = agregados.get("teses_n")
    if teses_media is not None and teses_total:
        ce_teses = ce_r.get("teses_media_anos")
        ce_txt = (f" (CE: {ce_teses:.1f} anos)" if ce_teses is not None
                  else " (CE: N/A)" if ce_individual else "")
        linhas.append(f"- Duração média de conclusão de tese: {teses_media:.1f} anos (N={teses_total} teses){ce_txt}")

    # -- Resultados: empregabilidade --
    linhas.append("### Resultados: empregabilidade")
    _fmt("Empregabilidade na área do CE", "empregabilidade_area_pct")

    return "\n".join(linhas)
