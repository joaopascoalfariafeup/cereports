# Memória do projecto CEReports

## Projecto
Ferramenta web Flask que gera pareceres sobre relatórios pedagógicos de Ciclos de Estudos (CEs/cursos) da FEUP.
Repositório GitHub: joaopascoalfariafeup/cereports
VM: coursemonitoring@coursemonitoring.fe.up.pt (deploy automático via deployer.sh)
URL público: https://ce.uc-reports.com (cloudflared tunnel, porta 5001)

## Ficheiros principais
- `app_web.py` — Flask app: login → selecção CE+ano+PDF → progresso SSE → preview parecer
- `ce_core.py` — pipeline: PDF bytes → LLM → preview_payload dict
- `llm_ce.py` — chamadas LLM (Anthropic PDF nativo / OpenAI-compat text extraction)
- `sigarra.py` — SigarraSession (auth SIGARRA, idêntico ao UCReports)
- `sigarra_ce.py` — `listar_ces_publicos()` (scraping público, sem auth, cache 1h) + stub `extrair_relatorio_ce_pdf()`
- `logger.py` — AuditoriaLogger (idêntico ao UCReports)
- `prompts/system_prompt.txt` — system prompt para geração de pareceres (HTML, ~4000 chars)
- `.env.public` — configuração pública versionada; `.env` — segredos (não versionado)
- `ucreports-ce.service` — systemd service (waitress, porta 5001)
- `deployer.sh` + `deployer.service` — auto-deploy por polling do GitHub

## Deploy / VM
- Serviço: `ucreports-ce` (waitress, porta 5001)
- Deployer: `ucreports-ce-deployer`
- venv: `~/cereports-venv`
- `deployer.sh` faz `git pull` + `pip install -r requirements.txt` + `systemctl restart ucreports-ce`
- cloudflared: adicionar `ce.uc-reports.com → http://127.0.0.1:5001` em `~/.cloudflared/config.yml`
- cloudflared é gerido pelo serviço systemd `ucreports` (que corre `arrancar.sh` com tunnel, nome **auditoria-uc**). `sudo systemctl restart cloudflared` dá erro (não há serviço com esse nome). Para reiniciar o tunnel: `sudo systemctl restart ucreports` (reinicia waitress UCReports porta 5000 + cloudflared).
- Para expor um novo hostname, além de editar `~/.cloudflared/config.yml`, é necessário criar o CNAME no DNS do Cloudflare. UUID do tunnel: `e258d416-c8fe-4289-8b80-7a3163a531da`. Forma mais simples: painel Cloudflare → uc-reports.com → DNS → CNAME `ce` → `e258d416-c8fe-4289-8b80-7a3163a531da.cfargotunnel.com` (Proxied). Alternativa CLI: `cloudflared tunnel login` (obtém cert.pem) depois `cloudflared tunnel route dns auditoria-uc ce.uc-reports.com`.

## Arquitectura
- Fluxo: login SIGARRA → /ces (CE + ano + PDF upload) → /start POST → /progress SSE → /preview parecer
- Jobs em background thread; SSE streama o log filtrado (só FASE/ERRO/AVIS)
- Anthropic: PDF enviado como document base64 nativo (melhor compreensão de layout)
- OpenAI/IAedu: texto extraído via pypdf e enviado como texto
- Output salvo em `output/<job_id>/`: relatorio_ce.pdf, parecer.html, preview_payload.json, auditoria.log
- Sem submissão automática ao SIGARRA (para MVP)

## Projecto irmão
- UCReports (relatórios de UCs): `c:\Dados\Python\AuditoriaRelatoriosUC\`
- GitHub: joaopascoalfariafeup/ucreports
- URL: https://app.uc-reports.com (porta 5000)
- Serviço systemd `ucreports` corre `arrancar.sh` **com tunnel** → gere waitress (porta 5000) + cloudflared (tunnel auditoria-uc)
- `sudo systemctl restart ucreports` reinicia o UCReports E o cloudflared (usar após editar config.yml)
