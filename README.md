# Assistente de Pareceres de Ciclos de Estudos (CEs) — FEUP

Aplicação web para geração assistida por IA de pareceres pedagógicos sobre
relatórios de Ciclos de Estudos da FEUP, com autenticação via SIGARRA.

## Funcionalidades

- **Autenticação SIGARRA** — login com credenciais institucionais ou autenticação federada UP (OIDC/Keycloak)
- **Seleção de CE e ano letivo** — lista pública de CEs scrapeada do SIGARRA
- **Permissões automáticas** — os CEs disponíveis dependem dos cargos do utilizador:
  - Membros do **Conselho Pedagógico** (CP): licenciaturas e mestrados
  - Membros do **Conselho Científico** (CC): licenciaturas, mestrados e doutoramentos
  - Membros da **Comissão de Acompanhamento** (CA): doutoramento específico
  - **Diretores de curso**: podem solicitar auto-avaliação do seu CE
- **Geração de parecer por LLM** — suporta Anthropic Claude e IAedu (FCT/FCCN)
- **Revisão do parecer** — editor com contador de caracteres (limite 10 000)
- **Submissão no SIGARRA** — submete o parecer diretamente no SIGARRA (login por password ou admin)
- **Notificação por email** — notifica membros da comissão para revisão no SIGARRA (via Resend)
- **Exportação** — guardar texto ou ZIP (parecer + relatório original + log de auditoria)
- **Controlo de custos** por utilizador (limite mensal configurável)
- **Modo administrador** — impersonação de qualquer utilizador para testes

## Estrutura de ficheiros

```
app_web.py          # Flask app principal (porta 5001, waitress)
sigarra_ce.py       # Scraping SIGARRA: CEs, relatórios, cargos
sigarra.py          # Sessão autenticada SIGARRA
llm_ce.py           # Integração com LLMs (Anthropic, OpenAI, IAedu)
ce_core.py          # Pipeline: HTML relatório → LLM → parecer
logger.py           # Logger dual (terminal + ficheiro), métricas LLM
prompts/            # Prompt de sistema do LLM (system_prompt.txt)
requirements.txt    # Dependências Python
.env.public         # Configuração pública (versionada)
.env                # Segredos — NÃO versionado
deployer.sh         # Auto-deploy via git pull (corre como serviço)
deployer.service    # Unidade systemd do deployer
ucreports-ce.service # Unidade systemd da aplicação
install.txt         # Instruções de instalação no servidor
```

## Configuração

### Variáveis de ambiente (`.env`)

| Variável | Descrição |
|---|---|
| `WEB_SECRET_KEY` | Chave secreta Flask (obrigatória) |
| `ANTHROPIC_API_KEY` | Chave API Anthropic |
| `IAEDU_API_KEY` | Chave API IAedu |
| `IAEDU_ENDPOINT` | Endpoint IAedu |
| `IAEDU_ID_CANAL` | Canal IAedu |
| `OIDC_CLIENT_SECRET` | Segredo do cliente OIDC Keycloak UP |
| `SIGARRA_SERVER_LOGIN` | Login SIGARRA do servidor (para sessão clone em OIDC) |
| `SIGARRA_SERVER_PASSWORD` | Password SIGARRA do servidor |
| `RESEND_API_KEY` | Chave API Resend (para notificações por email) |
| `ADMIN_CODES` | Códigos SIGARRA de administradores (csv) |
| `WEB_COST_BYPASS_USERS` | Utilizadores isentos de limite de custo (csv) |

### Configuração pública (`.env.public`)

| Variável | Default | Descrição |
|---|---|---|
| `WEB_MAX_USD_PER_USER_PER_MONTH` | `5` | Limite mensal de custo LLM por utilizador (USD) |
| `WEB_FREE_LLM_PROVIDERS` | `iaedu` | Providers gratuitos (sempre disponíveis) |
| `WEB_LLM_PROVIDER_OPTIONS` | — | Providers disponíveis na UI (csv) |
| `WEB_LLM_MODEL_OPTIONS_JSON` | — | Modelos por provider (JSON) |
| `WEB_MAX_RUNNING_JOBS` | `8` | Jobs LLM simultâneos máximos |
| `WEB_OUTPUT_RETENTION_HOURS` | `2` | Retenção de ficheiros de output (horas) |
| `OIDC_CLIENT_ID` | — | Client ID OIDC Keycloak UP (vazio = desativado) |
| `OIDC_REDIRECT_URI` | — | Redirect URI OIDC |
| `RESEND_FROM` | `noreply@ce.uc-reports.com` | Remetente para emails de notificação |

## Instalação

Ver `install.txt` para instruções detalhadas de instalação em servidor Linux.

O serviço corre em `http://localhost:5001` por defeito, tipicamente atrás de
um proxy Cloudflare Tunnel ou nginx.

## Deploy automático

O `deployer.sh` corre em background como serviço systemd e monitoriza o
repositório GitHub. Quando deteta novos commits:

1. Ativa modo de manutenção (`.draining`)
2. Aguarda conclusão de jobs em curso
3. Faz `git pull` e atualiza dependências
4. Reinicia o serviço
5. Remove o modo de manutenção

## Providers LLM suportados

| Provider | Modelos típicos | Custo |
|---|---|---|
| IAedu (FCT/FCCN) | gpt-4o | Gratuito |
| Anthropic | claude-opus-4-6, claude-sonnet-4-6 | Pago |

## Fluxo de utilização

1. **Seleção** — escolher CE, ano letivo, perspetiva e provider LLM
2. **Geração** — o relatório é obtido do SIGARRA e enviado ao LLM (pareceres existentes são removidos do HTML para não influenciar)
3. **Revisão** — editar o parecer gerado (limite 10 000 caracteres)
4. **Submissão** — submeter no SIGARRA e opcionalmente notificar membro da comissão por email
