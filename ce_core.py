"""
Pipeline de análise de Ciclos de Estudos (CEs).

Orquestra: HTML relatório → LLM → preview_payload
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from logger import AuditoriaLogger
from llm_ce import analisar_relatorio_ce

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROMPTS_DIR = _SCRIPT_DIR / "prompts"


def analisar_ce(
    relatorio_html: str,
    ce_nome: str,
    ano_letivo: str,
    provider: str,
    modelo: str,
    run_dir: Path,
    logger: AuditoriaLogger,
    pareceres_anteriores: str | None = None,
    perspetiva: str = "",
) -> dict:
    """Pipeline de análise de um CE: HTML relatório → LLM → preview_payload.

    Args:
        relatorio_html: HTML limpo do relatório (obtido via SIGARRA).
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
    custo_pre = logger.total_custo_estimado() or 0.0
    try:
        parecer_html = analisar_relatorio_ce(
            relatorio_html=relatorio_html,
            ce_nome=ce_nome,
            ano_letivo=ano_letivo,
            provider=provider,
            modelo=modelo,
            logger=logger,
            pareceres_anteriores=pareceres_anteriores,
            perspetiva=perspetiva,
        )
        custo_post = logger.total_custo_estimado() or 0.0
        custo_str = f" [~${custo_post - custo_pre:.4f}]" if custo_post > custo_pre else ""
        logger.concluir_fase("llm", f"Parecer gerado{custo_str}")
    except Exception as e:
        logger.concluir_fase("llm", f"Falha ao gerar parecer: {e}", ok=False)
        raise

    # Guardar HTML do parecer
    (run_dir / "parecer.html").write_text(parecer_html, encoding="utf-8")

    # Guardar user_prompt para auditoria (visível no ZIP)
    _PERSPETIVA_LABELS = {
        "CC": "Conselho Científico (CC)",
        "CP": "Conselho Pedagógico (CP)",
        "CA": "Comissão de Acompanhamento (CA)",
        "DCE": "Diretor do Ciclo de Estudos — Auto-avaliação (DCE)",
    }
    perspetiva_label = _PERSPETIVA_LABELS.get((perspetiva or "").upper().strip(), "")
    user_prompt_txt = (
        f"Por favor, elabora um parecer ao relatório do ciclo de estudos "
        f'"{ce_nome}", ano letivo {ano_letivo}, com base no relatório fornecido.'
    )
    if perspetiva_label:
        user_prompt_txt += f"\n\nPerspetiva do parecer: {perspetiva_label}"
    if pareceres_anteriores:
        user_prompt_txt += (
            f"\n\n## Pareceres emitidos no relatório do ano letivo anterior\n\n"
            f"{pareceres_anteriores}"
        )
    user_prompt_txt += (
        f"\n\n## Conteúdo do relatório (HTML)\n\n"
        f"[ver relatorio_ce.html — {len(relatorio_html) // 1024} KB]"
    )
    (run_dir / "user_prompt.txt").write_text(user_prompt_txt, encoding="utf-8")

    # Copiar system_prompt para auditoria (visível no ZIP)
    for sp in [_PROMPTS_DIR / "system_prompt.txt", _SCRIPT_DIR / "system_prompt.txt"]:
        if sp.exists():
            shutil.copy(sp, run_dir / "system_prompt.txt")
            break

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
