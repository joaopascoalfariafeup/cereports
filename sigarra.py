"""
Módulo de acesso ao SIGARRA da Universidade do Porto.

Gere a autenticação, sessão com cookies e extração de dados de fichas de UC,
sumários e resultados estatísticos.
1) Aceder a URL de autenticação do SIGARRA - responde com redirect para o IdP - seguir cabeçalho "location" do redirect;
2) Aceder à pagina de autenticação (com os dados do redirect);
3) Fazer web scrap à página de autenticação e submeter o formulário;
4) A resposta é um redirect para o SIGARRA - fazer o pedido ao sigarra com os dados do redirect;
5) O SIGARRA responde com um cookie de sessão, entre outras coisas;
6) O cookie de sessão pode ser usado nos pedidos seguintes.

https://www.up.pt/pdados/
https://www.up.pt/pdados/new-request

"""



import getpass
import http.cookiejar
import io
import json
import math
import os
import random
import secrets
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from html.parser import HTMLParser
import html as html_mod
from bs4 import BeautifulSoup


# Diretório do script (para localizar .env)
_SCRIPT_DIR = Path(__file__).resolve().parent

# URLs do SIGARRA
SIGARRA_BASE = "https://sigarra.up.pt/feup/pt"
SIGARRA_AUTH_URL = f"{SIGARRA_BASE}/mob_val_geral.autentica"


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

_ENV_LOADED = False

def load_env():
    """Carrega variáveis de .env (segredos) e .env.public (config pública) para os.environ.

    Prioridade: variáveis de sistema > .env > .env.public
    Só define variáveis que ainda não estejam no ambiente, para que
    variáveis de sistema reais tenham sempre a precedência mais alta.
    """

    global _ENV_LOADED
    if _ENV_LOADED:
        return

    def _carregar(path: "Path"):
        if not path.is_file():
            return
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)

    # Carregar por ordem decrescente de prioridade (setdefault: primeiro vence)
    _carregar(_SCRIPT_DIR / ".env")         # segredos — maior prioridade
    _carregar(_SCRIPT_DIR / ".env.public")  # configuração pública — valores por omissão

    _ENV_LOADED = True

class _HTMLToText(HTMLParser):
    """Conversor de HTML para texto, preservando hierarquia de listas."""

    def __init__(self):
        super().__init__()
        self._parts: list[str] = []
        self._list_depth = 0

    def handle_data(self, data: str):
        self._parts.append(data)

    def handle_starttag(self, tag: str, attrs):
        if tag in ("ul", "ol"):
            self._list_depth += 1
        elif tag == "li":
            indent = "  " * max(0, self._list_depth - 1)
            self._parts.append(f"\n{indent}- ")
        elif tag == "br":
            if self._list_depth == 0:
                self._parts.append("\n")
        elif tag in ("p", "div"):
            self._parts.append("\n")

    def handle_endtag(self, tag: str):
        if tag in ("ul", "ol"):
            self._list_depth = max(0, self._list_depth - 1)
            if self._list_depth == 0:
                self._parts.append("\n")
        elif tag in ("p", "div"):
            self._parts.append("\n")

    def get_text(self) -> str:
        raw = "".join(self._parts)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def html_to_text(html_fragment: str) -> str:
    """Converte um fragmento HTML em texto plano."""
    parser = _HTMLToText()
    parser.feed(html_fragment)
    return parser.get_text()


# ---------------------------------------------------------------------------
# Sessão autenticada
# ---------------------------------------------------------------------------

class SigarraSession:
    """Sessão autenticada no SIGARRA com gestão automática de cookies."""

    def __init__(self):
        load_env()
        self._cookie_jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._cookie_jar)
        )
        self._lock = threading.Lock()  # serializa acessos ao _opener / _cookie_jar
        self._autenticado = False
        self._codigo_pessoal: str | None = None  # nº mecanográfico
        self._http_retries = int(os.environ.get("SIGARRA_HTTP_RETRIES", "2"))
        self._http_backoff_base = float(os.environ.get("SIGARRA_HTTP_BACKOFF_BASE", "0.7"))

    @property
    def autenticado(self) -> bool:
        return self._autenticado

    @property
    def codigo_pessoal(self) -> str | None:
        """Número mecanográfico do utilizador (capturado na autenticação)."""
        return self._codigo_pessoal

    def autenticar(self, login: str | None = None, password: str | None = None):
        """Autentica no SIGARRA.

        Ordem de prioridade para credenciais:
        1. Argumentos diretos (login, password)
        2. Ficheiro .env local (SIGARRA_LOGIN / SIGARRA_PASSWORD)
        3. Variáveis de ambiente do sistema
        4. Prompt interativo
        """
        login = login or os.environ.get("SIGARRA_LOGIN") or input("Login SIGARRA: ")
        password = (
            password
            or os.environ.get("SIGARRA_PASSWORD")
            or getpass.getpass("Password SIGARRA: ")
        )

        params = urllib.parse.urlencode({"pv_login": login, "pv_password": password})
        url = f"{SIGARRA_AUTH_URL}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        try:
            resp = self.http_open(req, timeout=30, context="autenticação SIGARRA")
            charset = resp.headers.get_content_charset() or "utf-8"
            body = resp.read().decode(charset)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            try:
                dados = json.loads(body)
                msg = dados.get("erro_msg", body)
            except json.JSONDecodeError:
                msg = f"HTTP {e.code}: {body[:200]}"
            raise PermissionError(f"Autenticação SIGARRA falhou: {msg}") from e

        dados = json.loads(body)
        if dados.get("erro") or not dados.get("authenticated"):
            raise PermissionError(
                f"Autenticação SIGARRA falhou: {dados.get('erro_msg', body)}"
            )

        self._autenticado = True

        # Guardar nº mecanográfico (código pessoal).
        self._codigo_pessoal = str(dados["codigo"]) if "codigo" in dados else None
        if not self._codigo_pessoal and login:
            m = re.match(r"[Uu][Pp](\d+)", login)
            if m:
                self._codigo_pessoal = m.group(1)

        return dados

    @classmethod
    def from_oidc_token(cls, access_token: str, codigo: str) -> "SigarraSession":
        """Troca access_token OIDC por sessão SIGARRA via GET Bearer.

        Endpoint: https://sigarra.up.pt/auth/oidc/token
        Requer que o access_token tenha o campo ``aud`` com o identificador
        do serviço SIGARRA (configuração do Audience Mapper no Keycloak UP).

        Raises:
            PermissionError: token inválido (HTTP 403) ou sem cookies.
            RuntimeError: erro HTTP (ex: 500 se aud em falta no token).
            ConnectionError: erro de rede.
        """
        sess = cls.__new__(cls)
        sess._cookie_jar = http.cookiejar.CookieJar()
        sess._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(sess._cookie_jar)
        )
        sess._lock = threading.Lock()
        sess._autenticado = False
        sess._codigo_pessoal = codigo
        sess._http_retries = 2
        sess._http_backoff_base = 0.7

        url = "https://sigarra.up.pt/auth/oidc/token"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Authorization": f"Bearer {access_token}",
        })
        try:
            with sess._lock:
                sess._opener.open(req, timeout=15)
            if not list(sess._cookie_jar):
                raise PermissionError("SIGARRA não devolveu cookies de sessão para o token OIDC")
            sess._autenticado = True
            return sess
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                body = ""
            if e.code == 403:
                raise PermissionError(f"Token OIDC rejeitado (HTTP 403){': ' + body if body else ''}") from e
            raise RuntimeError(f"Erro HTTP {e.code} ao trocar token OIDC{': ' + body if body else ''}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"Erro de rede ao contactar endpoint OIDC SIGARRA: {e}") from e

    def clone_para_utilizador(self, codigo: str) -> "SigarraSession":
        """Cria uma SigarraSession com os cookies desta sessão mas para um utilizador diferente.

        Usado quando o servidor tem a sua própria sessão SIGARRA e o utilizador
        autenticou por outro mecanismo (ex: Microsoft OAuth, email OTP).
        Os cookies são copiados cookie a cookie (deepcopy falha em RLock interno do jar).
        """
        import http.cookiejar
        import copy
        novo_jar = http.cookiejar.CookieJar()
        with self._lock:
            for c in self._cookie_jar:
                novo_jar.set_cookie(copy.copy(c))
        nova = SigarraSession.__new__(SigarraSession)
        nova._cookie_jar = novo_jar
        nova._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(novo_jar)
        )
        nova._lock = threading.Lock()
        nova._autenticado = True
        nova._codigo_pessoal = codigo
        nova._http_retries = self._http_retries
        nova._http_backoff_base = self._http_backoff_base
        return nova

    @staticmethod
    def _saml_input_val(html: str, name: str) -> str:
        """Extrai value de um campo de formulário pelo name."""
        m = re.search(
            rf'name=["\']?{re.escape(name)}["\']?[^>]+value=["\']([^"\']*)["\']',
            html, re.IGNORECASE,
        ) or re.search(
            rf'value=["\']([^"\']*)["\'][^>]+name=["\']?{re.escape(name)}["\']?',
            html, re.IGNORECASE,
        )
        return html_mod.unescape(m.group(1)) if m else ""

    @staticmethod
    def _saml_form_action(html: str, base_url: str) -> str:
        m = re.search(r'<form[^>]+action=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if m:
            return urllib.parse.urljoin(base_url, html_mod.unescape(m.group(1)))
        return base_url

    def _saml_request(self, url: str, post_data=None, referer: str | None = None) -> tuple[str, str]:
        """Executa um pedido HTTP no fluxo SAML, usando o opener com cookies da sessão."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        }
        if referer:
            headers["Referer"] = referer
        if post_data:
            encoded = urllib.parse.urlencode(post_data).encode("ascii")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            req = urllib.request.Request(url, data=encoded, headers=headers)
        else:
            req = urllib.request.Request(url, headers=headers)
        resp = self._opener.open(req, timeout=30)
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace"), resp.geturl()

    def autenticar_federado_iniciar(self) -> tuple[str, str]:
        """Inicia o fluxo SAML até ao formulário de login do IdP (e1s2).

        Returns:
            tuple (html_e1s2, url_e1s2) — HTML do formulário de login e URL base.
        """
        _FED_START = f"{SIGARRA_BASE}/vld_validacao.federate_login?p_redirect=web_page.Inicial"

        try:
            html_e1s1, url_e1s1 = self._saml_request(_FED_START)
        except Exception as e:
            raise ConnectionError(f"Autenticação federada: falha ao contactar SIGARRA/IdP: {e}") from e

        if "wayf.up.pt" not in url_e1s1:
            raise PermissionError(f"Autenticação federada: redirecionamento inesperado para {url_e1s1}")

        csrf_e1s1 = self._saml_input_val(html_e1s1, "csrf_token")
        if not csrf_e1s1:
            raise PermissionError("Autenticação federada: csrf_token não encontrado em e1s1")

        probe = {
            "csrf_token": csrf_e1s1,
            "shib_idp_ls_exception.shib_idp_session_ss": "",
            "shib_idp_ls_success.shib_idp_session_ss": "true",
            "shib_idp_ls_value.shib_idp_session_ss": "",
            "shib_idp_ls_exception.shib_idp_persistent_ss": "",
            "shib_idp_ls_success.shib_idp_persistent_ss": "true",
            "shib_idp_ls_value.shib_idp_persistent_ss": "",
            "shib_idp_ls_supported": "true",
            "_eventId_proceed": "",
        }
        try:
            html_e1s2, url_e1s2 = self._saml_request(url_e1s1, post_data=probe, referer=url_e1s1)
        except Exception as e:
            raise ConnectionError(f"Autenticação federada: falha na sonda localStorage (e1s1): {e}") from e

        return html_e1s2, url_e1s2

    def autenticar_federado_completar(self, html_saml: str, url_saml: str, username: str = "") -> None:
        """Completa o fluxo SAML extraindo a asserção e submetendo ao SIGARRA."""
        saml_response = self._saml_input_val(html_saml, "SAMLResponse")
        relay_state = self._saml_input_val(html_saml, "RelayState")
        saml_action = self._saml_form_action(html_saml, "https://sigarra.up.pt/Shibboleth.sso/SAML2/POST")

        if not saml_response:
            raise PermissionError("Autenticação federada: asserção SAML não encontrada")

        try:
            _html_final, url_final = self._saml_request(
                saml_action,
                post_data={"SAMLResponse": saml_response, "RelayState": relay_state},
                referer=url_saml,
            )
        except Exception as e:
            raise ConnectionError(f"Autenticação federada: falha ao submeter asserção SAML: {e}") from e

        if "sigarra.up.pt" not in url_final:
            raise PermissionError(f"Autenticação federada: SIGARRA não reconheceu a sessão ({url_final})")

        self._autenticado = True

        # Tentar extrair código pessoal
        m = re.match(r"[Uu][Pp](\d+)", username)
        if m:
            self._codigo_pessoal = m.group(1)
        else:
            try:
                html_home, _ = self._saml_request(f"{SIGARRA_BASE}/web_page.inicial", referer=url_final)
                mc = re.search(r'foto_thumb\?pct_cod=(\d+)', html_home)
                self._codigo_pessoal = mc.group(1) if mc else None
            except Exception:
                self._codigo_pessoal = None

    def fetch_html(self, url: str, timeout: int = 30) -> str:
        """Descarrega uma página do SIGARRA (com cookies de sessão)."""
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        try:
            resp = self.http_open(req, timeout=timeout, context=f"GET {url}")
            charset = resp.headers.get_content_charset() or "iso-8859-15"
            return resp.read().decode(charset, errors="replace")

        except urllib.error.HTTPError as e:
            try:
                charset = e.headers.get_content_charset() or "iso-8859-15"
                body = e.read().decode(charset, errors="replace")
            except Exception:
                body = ""

            if e.code == 401:
                raise PermissionError(f"Sessão expirada / não autenticado (HTTP 401)") from e
            if e.code == 403:
                raise PermissionError(f"Sem permissão para aceder a esta página (HTTP 403)") from e
            if e.code == 404:
                raise ValueError(f"Página não encontrada (404) ao aceder ao URL") from e

            raise RuntimeError(f"Erro HTTP {e.code} ao aceder a {url}. {body[:200]}") from e

        except urllib.error.URLError as e:
            raise ConnectionError(f"Erro de rede/timeout ao aceder a {url}: {e}") from e

    def post_form(self, url: str, data: "dict | list[tuple]", timeout: int = 30) -> str:
        """Faz POST de um formulário para o SIGARRA (com cookies de sessão).

        data pode ser dict ou lista de tuplos (para múltiplos valores da mesma chave).
        """
        encoded = urllib.parse.urlencode(data, doseq=True).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=encoded,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        try:
            with self._lock:
                resp = self._opener.open(req, timeout=timeout)
            charset = resp.headers.get_content_charset() or "iso-8859-15"
            return resp.read().decode(charset, errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise PermissionError("Sessão expirada (HTTP 401)") from e
            if e.code == 403:
                raise PermissionError("Sem permissão para submeter (HTTP 403)") from e
            raise RuntimeError(f"Erro HTTP {e.code} ao fazer POST para {url}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"Erro de rede ao fazer POST para {url}: {e}") from e

    @staticmethod
    def _is_retryable_http(code: int) -> bool:
        return code in {408, 429, 500, 502, 503, 504}

    def http_open(
        self,
        req: urllib.request.Request,
        timeout: int = 30,
        retries: int | None = None,
        context: str = "",
    ):
        """Abre request HTTP com retries e backoff para falhas transitórias."""
        tentativas = (self._http_retries if retries is None else max(0, retries)) + 1
        last_exc: Exception | None = None

        for i in range(1, tentativas + 1):
            try:
                with self._lock:
                    return self._opener.open(req, timeout=timeout)
            except urllib.error.HTTPError as e:
                last_exc = e
                if self._is_retryable_http(e.code) and i < tentativas:
                    atraso = self._http_backoff_base * (2 ** (i - 1)) + random.uniform(0, 0.2)
                    print(f"  [retry {i}/{tentativas-1}] HTTP {e.code} em {context or req.full_url}; novo intento em {atraso:.1f}s")
                    time.sleep(atraso)
                    continue
                raise
            except urllib.error.URLError as e:
                last_exc = e
                if i < tentativas:
                    atraso = self._http_backoff_base * (2 ** (i - 1)) + random.uniform(0, 0.2)
                    print(f"  [retry {i}/{tentativas-1}] erro de rede em {context or req.full_url}: {e}; novo intento em {atraso:.1f}s")
                    time.sleep(atraso)
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError("Falha inesperada em http_open")
