"""
Pipeline de análise de Ciclos de Estudos (CEs).

Orquestra: HTML relatório → LLM → preview_payload
"""

from __future__ import annotations

import json
from pathlib import Path

from logger import AuditoriaLogger
from llm_ce import analisar_relatorio_ce

_SCRIPT_DIR = Path(__file__).resolve().parent


def analisar_ce(
    relatorio_html: str,
    ce_nome: str,
    ano_letivo: str,
    provider: str,
    modelo: str,
    run_dir: Path,
    logger: AuditoriaLogger,
) -> dict:
    """Pipeline de análise de um CE: HTML relatório → LLM → preview_payload.

    Args:
        relatorio_html: HTML limpo do relatório pedagógico (obtido via SIGARRA).
        ce_nome:        Nome do ciclo de estudos.
        ano_letivo:     Ano letivo (ex: "2024/25").
        provider:       Provider LLM ("anthropic", "openai", "iaedu").
        modelo:         Modelo LLM.
        run_dir:        Diretório de saída para esta execução.
        logger:         Logger para progresso e metadados.

    Returns:
        Dict preview_payload com os resultados da análise.
    """
    run_dir.mkdir(parents=True, exist_ok=True)

    # Guardar HTML de entrada (para referência / auditoria)
    html_path = run_dir / "relatorio_ce.html"
    html_path.write_text(relatorio_html, encoding="utf-8")
    logger.info(f"  HTML do relatório guardado: {html_path.name} ({len(relatorio_html) // 1024} KB)")

    # Chamar LLM
    logger.iniciar_fase("llm", f"A gerar parecer ({provider} / {modelo})...")
    try:
        parecer_html = analisar_relatorio_ce(
            relatorio_html=relatorio_html,
            ce_nome=ce_nome,
            ano_letivo=ano_letivo,
            provider=provider,
            modelo=modelo,
            logger=logger,
        )
        logger.concluir_fase("llm", "Parecer gerado")
    except Exception as e:
        logger.concluir_fase("llm", f"Falha ao gerar parecer: {e}", ok=False)
        raise

    # Guardar HTML do parecer
    (run_dir / "parecer.html").write_text(parecer_html, encoding="utf-8")

    preview_payload = {
        "ce_nome": ce_nome,
        "ano_letivo": ano_letivo,
        "parecer_html": parecer_html,
        "provider": provider,
        "modelo": modelo,
    }

    (run_dir / "preview_payload.json").write_text(
        json.dumps(preview_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return preview_payload
