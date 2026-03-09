"""
Módulo de acesso ao SIGARRA para Ciclos de Estudos (CEs).

Funções específicas para listar CEs e obter relatórios pedagógicos.
"""

from __future__ import annotations

import re
from bs4 import BeautifulSoup

from sigarra import SigarraSession, SIGARRA_BASE


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
