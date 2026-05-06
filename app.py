from flask import Flask, jsonify, render_template, request, session, redirect, url_for
from functools import wraps
import sqlite3, os, requests, json, re, time
from datetime import datetime
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "iqvia-pharma-2026-xK9m")
app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024  # 300 MB
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "iqvia.db")

# ── Tabela real no SAP IQ ─────────────────────────────────────────────────
TABLE_PRESC = os.environ.get("TABLE_PRESC", "qqhetl.PBS_AI_ANALYTICS")

# ── Modo A: Conexão direta pymssql (túnel TCP claude.sqltech.com.br:3030) ─
_IQ_HOST = os.environ.get("IQ_HOST", "claude.sqltech.com.br")
_IQ_PORT = int(os.environ.get("IQ_PORT", "3030"))
_IQ_DB   = os.environ.get("IQ_DATABASE", "IQHML")
_IQ_USER = os.environ.get("IQ_USER", "iaapi")
_IQ_PASS = os.environ.get("IQ_PASSWORD", "i@sql2025HML")
USE_DIRECT = False  # pymssql incompatível com SAP IQ — usar HTTP API

# ── Agente Java via Cloudflare Tunnel ────────────────────────────────────
# URL muda a cada reinício do tunnel — atualizar AGENT_URL no Railway
_AGENT_URL     = os.environ.get("AGENT_URL", "https://membrane-hdtv-qui-casual.trycloudflare.com")
_AGENT_API_KEY = os.environ.get("AGENT_API_KEY", "")
USE_HTTP_API   = True  # sempre usa o agente Java

# Certificado de cliente mTLS (sqlsrv50.pfx, senha 1234)
_PFX_PATH    = os.path.join(os.path.dirname(__file__), "sqlsrv50.pfx")
_PFX_PASS    = os.environ.get("PFX_PASSWORD", "1234").encode()
_CLIENT_CERT = None   # (cert_pem, key_pem) para mTLS
_CA_CERT     = None   # ca.pem para verificar servidor

def _load_client_cert():
    """Extrai cert+key do .pfx.
    Tenta como certificado de cliente (mTLS) E como CA de servidor."""
    global _CLIENT_CERT, _CA_CERT
    if not os.path.exists(_PFX_PATH):
        print(f"[ssl] {_PFX_PATH} não encontrado.")
        return
    try:
        from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption
        with open(_PFX_PATH, "rb") as f:
            pfx_data = f.read()
        key, cert, extras = pkcs12.load_key_and_certificates(pfx_data, _PFX_PASS)
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)

        # Salva cert + key para uso como certificado de cliente
        cert_path = os.path.join(data_dir, "_client.crt")
        key_path  = os.path.join(data_dir, "_client.key")
        with open(cert_path, "wb") as f:
            f.write(cert.public_bytes(Encoding.PEM))
        with open(key_path, "wb") as f:
            f.write(key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()))
        _CLIENT_CERT = (cert_path, key_path)

        # Salva cert como CA para verificar o servidor
        ca_path = os.path.join(data_dir, "_ca.pem")
        with open(ca_path, "wb") as f:
            f.write(cert.public_bytes(Encoding.PEM))
            # inclui certificados extras da cadeia, se houver
            if extras:
                for c in extras:
                    f.write(c.public_bytes(Encoding.PEM))
        _CA_CERT = ca_path

        print(f"[ssl] Certificado carregado: {cert.subject}")
    except Exception as e:
        print(f"[ssl] Erro ao carregar certificado: {e}")

_load_client_cert()

def _direct_query(sql):
    """Conexão direta ao SAP IQ via pymssql (túnel TCP porta 3030)."""
    import pymssql
    conn = pymssql.connect(
        server=_IQ_HOST,
        port=_IQ_PORT,
        database=_IQ_DB,
        user=_IQ_USER,
        password=_IQ_PASS,
        login_timeout=15,
        timeout=30,
        as_dict=True
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return []

def adapt_sql(sql):
    """Redireciona 'prescricoes' → tabela real e converte LIMIT→TOP."""
    sql = re.sub(r'\bprescricoes\b', TABLE_PRESC, sql)
    m = re.search(r'\bLIMIT\s+(\d+)\s*;?\s*$', sql.strip(), re.IGNORECASE)
    if m:
        n   = m.group(1)
        sql = re.sub(r'\bLIMIT\s+\d+\s*;?\s*$', '', sql.strip(), flags=re.IGNORECASE).rstrip()
        sql = re.sub(r'^(\s*SELECT\s)', f'SELECT TOP {n} ', sql, flags=re.IGNORECASE, count=1)
    return sql

def _inline_params(sql, params):
    """Substitui ? pelos valores de forma segura para envio via API."""
    for p in params:
        if p is None:
            val = "NULL"
        elif isinstance(p, str):
            val = "'" + p.replace("'", "''") + "'"
        elif isinstance(p, (int, float)):
            val = str(p)
        else:
            val = "'" + str(p).replace("'", "''") + "'"
        sql = sql.replace("?", val, 1)
    return sql

def _api_call(sql):
    """Envia SQL ao Java Agent via Cloudflare Tunnel e retorna lista de dicts."""
    hdrs = {"Content-Type": "application/json"}
    if _AGENT_API_KEY:
        hdrs["X-API-Key"] = _AGENT_API_KEY
    resp = requests.post(
        f"{_AGENT_URL}/query",
        json={"sql": sql, "limit": 500},
        headers=hdrs,
        verify=True,
        timeout=180
    )
    resp.raise_for_status()
    data = resp.json()

    # Formato A: {"columns": [...], "rows": [[...], ...]}
    if isinstance(data, dict) and "columns" in data and "rows" in data:
        cols = data["columns"]
        return [dict(zip(cols, row)) for row in data["rows"]]

    # Formato B: [[col1, col2, ...], [val1, val2, ...], ...]  (1ª linha = cabeçalho)
    if isinstance(data, list) and data and isinstance(data[0], list):
        if len(data) == 1:
            # só uma linha de valores — sem header
            return [{"col_" + str(i): v for i, v in enumerate(data[0])}]
        cols = [str(c) for c in data[0]]
        return [dict(zip(cols, row)) for row in data[1:]]

    # Formato C: [{"col": val}, ...]  (já é lista de dicts)
    if isinstance(data, list):
        return data

    # Formato D: wrapper com chave conhecida
    for key in ("rows", "data", "results", "result"):
        if isinstance(data.get(key), list):
            return data[key]

    return []

def query(sql, params=()):
    final = adapt_sql(_inline_params(sql, params))
    if USE_DIRECT:
        return _direct_query(final)
    if USE_HTTP_API:
        return _api_call(final)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def execute(sql, params=()):
    final = adapt_sql(_inline_params(sql, params))
    if USE_DIRECT:
        _direct_query(final)
        return
    if USE_HTTP_API:
        _api_call(final)
        return
    con = sqlite3.connect(DB_PATH)
    con.execute(sql, params)
    con.commit()
    con.close()

def table_exists(name):
    try:
        rows = query(
            "SELECT COUNT(*) AS ex FROM information_schema.tables "
            "WHERE table_schema='qqhetl' AND table_name='PBS_AI_ANALYTICS'")
        return bool(rows[0].get("ex", 0))
    except Exception:
        return True  # assume existente em caso de erro

def _df_to_table(df, table_name):
    if USE_HTTP_API:
        print("[warn] Upload via API HTTP não suportado — operação ignorada.")
        return
    con = sqlite3.connect(DB_PATH)
    df.to_sql(table_name, con, if_exists="replace", index=False)
    con.close()

# ── Cache: L0 JSON file · L1 memória · L2 PostgreSQL ─────────────────────
# Chat: SQL direto no SAP IQ via Java Agent (sem cache).
# Market + Prescritores: servidos de arquivos JSON estáticos no repo.
#   → Nunca fazem query no carregamento da página.

_STATIC_DASH = os.path.join(os.path.dirname(__file__), "data", "dashboard.json")
_STATIC_RANK = os.path.join(os.path.dirname(__file__), "data", "ranking.json")

_cache    = {}
CACHE_TTL = 30 * 24 * 3600   # 30 dias


def _load_static_files():
    """
    L0 — carrega data/dashboard.json e data/ranking.json (commited no git).
    Se os arquivos existirem, o dashboard fica pronto em <1 ms sem nenhuma query.
    """
    loaded = 0
    if os.path.exists(_STATIC_DASH):
        try:
            with open(_STATIC_DASH, encoding="utf-8") as f:
                dash = json.load(f)
            cache_set("dash::():MANUFACTURER_DESC:STATE_DESC", dash)
            gen = dash.get("generated_at", "?")
            print(f"[static] dashboard.json carregado (gerado em {gen}).")
            loaded += 1
        except Exception as e:
            print(f"[static] Erro ao carregar dashboard.json: {e}")
    if os.path.exists(_STATIC_RANK):
        try:
            with open(_STATIC_RANK, encoding="utf-8") as f:
                rank = json.load(f)
            ranking = rank.get("ranking", rank) if isinstance(rank, dict) else rank
            cache_set("ranking::():200", ranking)
            gen = rank.get("generated_at", "?") if isinstance(rank, dict) else "?"
            print(f"[static] ranking.json carregado (gerado em {gen}).")
            loaded += 1
        except Exception as e:
            print(f"[static] Erro ao carregar ranking.json: {e}")
    if loaded == 0:
        print("[static] Nenhum arquivo JSON estático encontrado — usando prewarm.")
    return loaded

def cache_get(key):
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL:
        return entry["data"]
    return None

def cache_set(key, data):
    _cache[key] = {"data": data, "ts": time.time()}

def cache_clear():
    _cache.clear()

# Inicializa e carrega ao subir o servidor
# Ordem: L0 arquivo JSON → L1 memória → prewarm SAP IQ (só se necessário)
_load_static_files()   # carrega JSON do repo em <1ms, sem rede

# ── Auth ──────────────────────────────────────────────────────────────────
USERS = {
    "admin@iqvia.com":              {"password": "Iqvia2026",   "name": "Admin IQVIA"},
    "marcio.amorim@sqltech.com.br": {"password": "Sqltech123",  "name": "Márcio Amorim"},
    "fabio.chaves@iqvia.com":       {"password": "Fabio@2026",  "name": "Fábio Chaves"},
}

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated

@app.route("/login", methods=["GET", "POST"])
def login_page():
    if session.get("logged_in"):
        return redirect(url_for("market_page"))
    error = None
    if request.method == "POST":
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = USERS.get(email)
        if user and user["password"] == password:
            session["logged_in"]  = True
            session["user_email"] = email
            session["user_name"]  = user["name"]
            return redirect(url_for("market_page"))
        error = "E-mail ou senha incorretos."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))

# ── Init prescrições ──────────────────────────────────────────────────────
PRESCRICOES_CSV = os.path.join(os.path.dirname(__file__), "data", "prescricoes.csv")
os.makedirs(os.path.join(os.path.dirname(__file__), "data"), exist_ok=True)

def init_prescricoes():
    if USE_HTTP_API:
        print(f"[init] Modo HTTP API — usando tabela {TABLE_PRESC} via claude.sqltech.com.br.")
        return
    if table_exists("prescricoes"):
        print("[init] Tabela prescricoes já existe.")
        return
    if not os.path.exists(PRESCRICOES_CSV):
        print(f"[init] {PRESCRICOES_CSV} não encontrado.")
        return
    print("[init] Carregando prescricoes.csv...")
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(PRESCRICOES_CSV, skiprows=1, encoding=enc, header=0)
            break
        except UnicodeDecodeError:
            continue
    df.columns = ["crm","medico","periodo","canal","brick","cidade","estado",
                  "laboratorio","marca","molecula","qtde_med","qtde_rec"]
    df["molecula"]    = df["molecula"].str.strip()
    df["periodo"]     = df["periodo"].astype(str)
    df["qtde_med"]    = pd.to_numeric(df["qtde_med"], errors="coerce").fillna(0).astype(int)
    df["qtde_rec"]    = pd.to_numeric(df["qtde_rec"], errors="coerce").fillna(0).astype(int)
    _df_to_table(df, "prescricoes")
    print(f"[init] {len(df)} linhas carregadas.")

def ensure_indexes():
    # Via HTTP API não gerenciamos índices localmente; SAP IQ já tem seus próprios.
    pass

try:
    init_prescricoes()
    # ensure_indexes() não roda no startup para não bloquear o healthcheck;
    # é chamado automaticamente após o primeiro upload de dados.
except Exception as e:
    print(f"[init] Aviso: {e}")

# ── Helpers de filtro ─────────────────────────────────────────────────────
def build_filters(args):
    clauses, params = [], []
    mol_raw = args.get("molecula", "")
    mols = [m.strip() for m in mol_raw.split(",") if m.strip()]
    if mols:
        placeholders = ",".join(["?"] * len(mols))
        clauses.append(f"COMBINED_MOLECULE_DESC IN ({placeholders})")
        params.extend(mols)
    if args.get("laboratorio"):
        clauses.append("MANUFACTURER_DESC = ?"); params.append(args["laboratorio"])
    if args.get("estado"):
        clauses.append("STATE_DESC = ?"); params.append(args["estado"])
    if args.get("cidade"):
        clauses.append("CITY_DESC = ?"); params.append(args["cidade"])
    if args.get("brick"):
        clauses.append("IMS_BRICK_DESC = ?"); params.append(args["brick"])
    if args.get("periodo_ini"):
        clauses.append("PERIOD_CD >= ?"); params.append(args["periodo_ini"])
    if args.get("periodo_fim"):
        clauses.append("PERIOD_CD <= ?"); params.append(args["periodo_fim"])
    return " AND ".join(clauses), tuple(params)

# ── Filtros disponíveis ───────────────────────────────────────────────────
@app.route("/api/filters/all")
@login_required
def filter_all():
    """Retorna todos os filtros em uma única chamada."""
    cached = cache_get("filters_all")
    if cached:
        return jsonify(cached)
    mols     = query("SELECT DISTINCT COMBINED_MOLECULE_DESC AS molecula    FROM prescricoes WHERE COMBINED_MOLECULE_DESC IS NOT NULL ORDER BY COMBINED_MOLECULE_DESC")
    labs     = query("SELECT DISTINCT MANUFACTURER_DESC     AS laboratorio FROM prescricoes WHERE MANUFACTURER_DESC     IS NOT NULL ORDER BY MANUFACTURER_DESC")
    estados  = query("SELECT DISTINCT STATE_DESC            AS estado       FROM prescricoes WHERE STATE_DESC            IS NOT NULL ORDER BY STATE_DESC")
    periodos = query("SELECT DISTINCT PERIOD_CD             AS periodo      FROM prescricoes WHERE PERIOD_CD             IS NOT NULL ORDER BY PERIOD_CD")
    result = {
        "moleculas":    [r["molecula"]    for r in mols],
        "laboratorios": [r["laboratorio"] for r in labs],
        "estados":      [r["estado"]      for r in estados],
        "periodos":     [r["periodo"]     for r in periodos],
    }
    cache_set("filters_all", result)
    return jsonify(result)

@app.route("/api/filters/moleculas")
@login_required
def filter_moleculas():
    cached = cache_get("filter_moleculas")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT COMBINED_MOLECULE_DESC AS molecula FROM prescricoes WHERE COMBINED_MOLECULE_DESC IS NOT NULL ORDER BY COMBINED_MOLECULE_DESC")
    data = [r["molecula"] for r in rows]
    cache_set("filter_moleculas", data)
    return jsonify(data)

@app.route("/api/filters/laboratorios")
@login_required
def filter_laboratorios():
    mol_raw = request.args.get("molecula", "")
    mols = [m.strip() for m in mol_raw.split(",") if m.strip()]
    if mols:
        placeholders = ",".join(["?"] * len(mols))
        rows = query(f"SELECT DISTINCT MANUFACTURER_DESC AS laboratorio FROM prescricoes WHERE COMBINED_MOLECULE_DESC IN ({placeholders}) AND MANUFACTURER_DESC IS NOT NULL ORDER BY MANUFACTURER_DESC", tuple(mols))
        return jsonify([r["laboratorio"] for r in rows])
    cached = cache_get("filter_laboratorios")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT MANUFACTURER_DESC AS laboratorio FROM prescricoes WHERE MANUFACTURER_DESC IS NOT NULL ORDER BY MANUFACTURER_DESC")
    data = [r["laboratorio"] for r in rows]
    cache_set("filter_laboratorios", data)
    return jsonify(data)

@app.route("/api/filters/estados")
@login_required
def filter_estados():
    cached = cache_get("filter_estados")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT STATE_DESC AS estado FROM prescricoes WHERE STATE_DESC IS NOT NULL ORDER BY STATE_DESC")
    data = [r["estado"] for r in rows]
    cache_set("filter_estados", data)
    return jsonify(data)

@app.route("/api/filters/periodos")
@login_required
def filter_periodos():
    cached = cache_get("filter_periodos")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT PERIOD_CD AS periodo FROM prescricoes WHERE PERIOD_CD IS NOT NULL ORDER BY PERIOD_CD")
    data = [r["periodo"] for r in rows]
    cache_set("filter_periodos", data)
    return jsonify(data)

@app.route("/api/filters/cidades")
@login_required
def filter_cidades():
    estado = request.args.get("estado", "")
    if estado:
        rows = query("SELECT DISTINCT CITY_DESC AS cidade FROM prescricoes WHERE STATE_DESC=? AND CITY_DESC IS NOT NULL ORDER BY CITY_DESC", (estado,))
        return jsonify([r["cidade"] for r in rows])
    cached = cache_get("filter_cidades")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT CITY_DESC AS cidade FROM prescricoes WHERE CITY_DESC IS NOT NULL ORDER BY CITY_DESC")
    data = [r["cidade"] for r in rows]
    cache_set("filter_cidades", data)
    return jsonify(data)

# ── Market Intelligence ───────────────────────────────────────────────────
@app.route("/api/market/kpis")
@login_required
def market_kpis():
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    ck = f"kpis:{filters}:{params}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    r = query(f"""
        SELECT
            COALESCE(SUM(RX_COUNT_TOTAL), 0)           AS total_receitas,
            COALESCE(SUM(DISPENSED_QTY_TOTAL), 0)       AS total_medicamentos,
            COUNT(DISTINCT DOCTOR_DISPLAY_CD)            AS qtde_medicos,
            COUNT(DISTINCT MANUFACTURER_DESC)            AS qtde_laboratorios,
            COUNT(DISTINCT BRAND_NAME)                   AS qtde_marcas,
            COUNT(DISTINCT COMBINED_MOLECULE_DESC)       AS qtde_moleculas
        FROM prescricoes {w}
    """, params)
    cache_set(ck, r[0])
    return jsonify(r[0])

@app.route("/api/market/share")
@login_required
def market_share():
    _share_col_map = {
        "laboratorio": "MANUFACTURER_DESC",
        "marca":       "BRAND_NAME",
        "molecula":    "COMBINED_MOLECULE_DESC",
    }
    group_by = request.args.get("group_by", "laboratorio")
    if group_by not in _share_col_map:
        group_by = "laboratorio"
    col = _share_col_map[group_by]
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    ck = f"share:{group_by}:{filters}:{params}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    # ORDER BY expressão (não alias) — Sybase IQ não aceita ORDER BY alias
    # Share calculado em Python para evitar window function SUM(SUM()) OVER ()
    rows = query(f"""
        SELECT {col}                              AS nome,
               SUM(RX_COUNT_TOTAL)               AS receitas,
               SUM(DISPENSED_QTY_TOTAL)           AS medicamentos,
               COUNT(DISTINCT DOCTOR_DISPLAY_CD)  AS medicos
        FROM prescricoes {w}
        GROUP BY {col}
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
        LIMIT 15
    """, params)
    total_rx = sum(r.get("receitas") or 0 for r in rows) or 1
    rows = [{**r, "share": round((r.get("receitas") or 0) * 100.0 / total_rx, 2)} for r in rows]
    cache_set(ck, rows)
    return jsonify(rows)

@app.route("/api/market/evolucao")
@login_required
def market_evolucao():
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    ck = f"evolucao:{filters}:{params}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    rows = query(f"""
        SELECT PERIOD_CD                AS periodo,
               SUM(RX_COUNT_TOTAL)     AS receitas,
               SUM(DISPENSED_QTY_TOTAL) AS medicamentos
        FROM prescricoes {w}
        GROUP BY PERIOD_CD
        ORDER BY PERIOD_CD
    """, params)
    cache_set(ck, rows)
    return jsonify(rows)

@app.route("/api/market/geografico")
@login_required
def market_geografico():
    _geo_col_map = {
        "estado": "STATE_DESC",
        "cidade": "CITY_DESC",
        "brick":  "IMS_BRICK_DESC",
    }
    group_by = request.args.get("group_by", "estado")
    if group_by not in _geo_col_map:
        group_by = "estado"
    col = _geo_col_map[group_by]
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    ck = f"geo:{group_by}:{filters}:{params}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    rows = query(f"""
        SELECT {col}                         AS regiao,
               SUM(RX_COUNT_TOTAL)           AS receitas,
               SUM(DISPENSED_QTY_TOTAL)      AS medicamentos,
               COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
        FROM prescricoes {w}
        GROUP BY {col}
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
        LIMIT 20
    """, params)
    cache_set(ck, rows)
    return jsonify(rows)

# ── Dashboard: serve arquivo estático se existir, senão queries ao vivo ──
@app.route("/api/dashboard")
@login_required
def dashboard_combined():
    # Arquivo gerado → serve direto, sem nenhuma query
    if os.path.exists(_STATIC_DASH):
        with open(_STATIC_DASH, encoding="utf-8") as f:
            content = f.read()
        return app.response_class(content, mimetype="application/json")

    # Sem arquivo → busca ao vivo no SAP IQ
    try:
        kpis_r = query("""
            SELECT COALESCE(SUM(RX_COUNT_TOTAL),0) AS total_receitas,
                   COALESCE(SUM(DISPENSED_QTY_TOTAL),0) AS total_medicamentos,
                   COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS qtde_medicos,
                   COUNT(DISTINCT MANUFACTURER_DESC) AS qtde_laboratorios,
                   COUNT(DISTINCT BRAND_NAME) AS qtde_marcas,
                   COUNT(DISTINCT COMBINED_MOLECULE_DESC) AS qtde_moleculas
            FROM prescricoes
        """)
        evolucao = query("""
            SELECT PERIOD_CD AS periodo,
                   SUM(RX_COUNT_TOTAL) AS receitas,
                   SUM(DISPENSED_QTY_TOTAL) AS medicamentos
            FROM prescricoes GROUP BY PERIOD_CD ORDER BY PERIOD_CD
        """)
        share_raw = query("""
            SELECT MANUFACTURER_DESC AS nome,
                   SUM(RX_COUNT_TOTAL) AS receitas,
                   SUM(DISPENSED_QTY_TOTAL) AS medicamentos,
                   COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
            FROM prescricoes GROUP BY MANUFACTURER_DESC
            ORDER BY SUM(RX_COUNT_TOTAL) DESC LIMIT 15
        """)
        total_rx = sum(r.get("receitas") or 0 for r in share_raw) or 1
        share = [{**r, "share": round((r.get("receitas") or 0)*100.0/total_rx, 2)} for r in share_raw]
        geo = query("""
            SELECT STATE_DESC AS regiao,
                   SUM(RX_COUNT_TOTAL) AS receitas,
                   SUM(DISPENSED_QTY_TOTAL) AS medicamentos,
                   COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
            FROM prescricoes GROUP BY STATE_DESC
            ORDER BY SUM(RX_COUNT_TOTAL) DESC LIMIT 20
        """)
        return jsonify({"kpis": kpis_r[0] if kpis_r else {}, "evolucao": evolucao,
                        "share": share, "geo": geo})
    except Exception as e:
        return jsonify({"erro": str(e)}), 503


def _prewarm_cache():
    """
    Roda automaticamente no startup em background.
    Se os arquivos JSON estáticos (L0) ou PostgreSQL (L2) já popularam o cache,
    pula as queries pesadas — o dashboard fica pronto em ms.
    Só executa queries no SAP IQ se não houver dados em nenhuma camada.
    """
    import threading
    def _run():
        time.sleep(8)  # aguarda servidor subir + _load_static_files
        try:
            print("[prewarm] Verificando cache...")
            with app.app_context():
                ck_d    = "dash::():MANUFACTURER_DESC:STATE_DESC"
                ck_rank = "ranking::():200"
                dash_ok    = cache_get(ck_d)    is not None
                ranking_ok = cache_get(ck_rank) is not None

                if dash_ok and ranking_ok:
                    print("[prewarm] Cache já populado (JSON/PG) — nenhuma query necessária.")
                    return

                print("[prewarm] Cache incompleto — buscando dados do SAP IQ...")

                # Dashboard principal (sem filtros)
                if not dash_ok:
                    try:
                        kpis_r = query("""
                            SELECT COALESCE(SUM(RX_COUNT_TOTAL),0)       AS total_receitas,
                                   COALESCE(SUM(DISPENSED_QTY_TOTAL),0)  AS total_medicamentos,
                                   COUNT(DISTINCT DOCTOR_DISPLAY_CD)      AS qtde_medicos,
                                   COUNT(DISTINCT MANUFACTURER_DESC)      AS qtde_laboratorios,
                                   COUNT(DISTINCT BRAND_NAME)             AS qtde_marcas,
                                   COUNT(DISTINCT COMBINED_MOLECULE_DESC) AS qtde_moleculas
                            FROM prescricoes
                        """)
                        evolucao = query("""
                            SELECT PERIOD_CD AS periodo,
                                   SUM(RX_COUNT_TOTAL) AS receitas,
                                   SUM(DISPENSED_QTY_TOTAL) AS medicamentos
                            FROM prescricoes GROUP BY PERIOD_CD ORDER BY PERIOD_CD
                        """)
                        share_raw = query("""
                            SELECT MANUFACTURER_DESC AS nome,
                                   SUM(RX_COUNT_TOTAL) AS receitas,
                                   SUM(DISPENSED_QTY_TOTAL) AS medicamentos,
                                   COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
                            FROM prescricoes
                            GROUP BY MANUFACTURER_DESC
                            ORDER BY SUM(RX_COUNT_TOTAL) DESC
                            LIMIT 15
                        """)
                        total_rx = sum(r.get("receitas") or 0 for r in share_raw) or 1
                        share = [{**r, "share": round((r.get("receitas") or 0)*100.0/total_rx, 2)} for r in share_raw]
                        geo = query("""
                            SELECT STATE_DESC AS regiao,
                                   SUM(RX_COUNT_TOTAL) AS receitas,
                                   SUM(DISPENSED_QTY_TOTAL) AS medicamentos,
                                   COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
                            FROM prescricoes
                            GROUP BY STATE_DESC
                            ORDER BY SUM(RX_COUNT_TOTAL) DESC
                            LIMIT 20
                        """)
                        dash_data = {"kpis": kpis_r[0] if kpis_r else {}, "evolucao": evolucao, "share": share, "geo": geo}
                        cache_set(ck_d, dash_data)
                        # Salva também no arquivo JSON para próximos deploys
                        try:
                            os.makedirs(os.path.dirname(_STATIC_DASH), exist_ok=True)
                            dash_data["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                            with open(_STATIC_DASH, "w", encoding="utf-8") as ff:
                                json.dump(dash_data, ff, ensure_ascii=False, default=str)
                            print("[prewarm] dashboard.json atualizado.")
                        except Exception as we:
                            print(f"[prewarm] Aviso: não foi possível salvar dashboard.json: {we}")
                        print("[prewarm] Dashboard OK.")
                    except Exception as e:
                        print(f"[prewarm] Dashboard falhou: {e}")

                # Ranking de prescritores
                if not ranking_ok:
                    try:
                        ranking = query("""
                            SELECT DOCTOR_DISPLAY_CD                             AS crm,
                                   TRIM(FIRST_NM) || ' ' || TRIM(SURNM_NM)     AS medico,
                                   CITY_DESC AS cidade, STATE_DESC AS estado, IMS_BRICK_DESC AS brick,
                                   SUM(RX_COUNT_TOTAL)                          AS total_receitas,
                                   SUM(DISPENSED_QTY_TOTAL)                     AS total_medicamentos,
                                   COUNT(DISTINCT MANUFACTURER_DESC)             AS qtde_labs,
                                   COUNT(DISTINCT BRAND_NAME)                   AS qtde_marcas
                            FROM prescricoes
                            GROUP BY DOCTOR_DISPLAY_CD, FIRST_NM, SURNM_NM, CITY_DESC, STATE_DESC, IMS_BRICK_DESC
                            ORDER BY SUM(RX_COUNT_TOTAL) DESC
                            LIMIT 200
                        """)
                        cache_set(ck_rank, ranking)
                        try:
                            rank_data = {"ranking": ranking, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
                            with open(_STATIC_RANK, "w", encoding="utf-8") as ff:
                                json.dump(rank_data, ff, ensure_ascii=False, default=str)
                            print("[prewarm] ranking.json atualizado.")
                        except Exception as we:
                            print(f"[prewarm] Aviso: não foi possível salvar ranking.json: {we}")
                        print(f"[prewarm] Ranking OK: {len(ranking)} médicos.")
                    except Exception as e:
                        print(f"[prewarm] Ranking falhou: {e}")

        except Exception as e:
            print(f"[prewarm] Erro geral: {e}")
    threading.Thread(target=_run, daemon=True).start()

_prewarm_cache()

# ── Admin: gerar dados estáticos ─────────────────────────────────────────
@app.route("/admin/generate-static")
@login_required
def admin_generate_static():
    """
    Roda as queries no SAP IQ e salva data/dashboard.json + data/ranking.json.
    Chamar após atualização mensal dos dados.
    Retorna JSON com resumo do que foi gerado.
    """
    import threading, queue
    result_q = queue.Queue()

    def _gen():
        report = {"ok": False, "erros": [], "dados": {}}
        try:
            now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            # KPIs
            kpis_r = query("""
                SELECT COALESCE(SUM(RX_COUNT_TOTAL),0)       AS total_receitas,
                       COALESCE(SUM(DISPENSED_QTY_TOTAL),0)  AS total_medicamentos,
                       COUNT(DISTINCT DOCTOR_DISPLAY_CD)      AS qtde_medicos,
                       COUNT(DISTINCT MANUFACTURER_DESC)      AS qtde_laboratorios,
                       COUNT(DISTINCT BRAND_NAME)             AS qtde_marcas,
                       COUNT(DISTINCT COMBINED_MOLECULE_DESC) AS qtde_moleculas
                FROM prescricoes
            """)
            kpis = kpis_r[0] if kpis_r else {}

            # Evolução
            evolucao = query("""
                SELECT PERIOD_CD AS periodo,
                       SUM(RX_COUNT_TOTAL) AS receitas,
                       SUM(DISPENSED_QTY_TOTAL) AS medicamentos
                FROM prescricoes GROUP BY PERIOD_CD ORDER BY PERIOD_CD
            """)

            # Share
            share_raw = query("""
                SELECT MANUFACTURER_DESC AS nome,
                       SUM(RX_COUNT_TOTAL) AS receitas,
                       SUM(DISPENSED_QTY_TOTAL) AS medicamentos,
                       COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
                FROM prescricoes
                GROUP BY MANUFACTURER_DESC
                ORDER BY SUM(RX_COUNT_TOTAL) DESC
                LIMIT 15
            """)
            total_rx = sum(r.get("receitas") or 0 for r in share_raw) or 1
            share = [{**r, "share": round((r.get("receitas") or 0)*100.0/total_rx, 2)} for r in share_raw]

            # Geo
            geo = query("""
                SELECT STATE_DESC AS regiao,
                       SUM(RX_COUNT_TOTAL) AS receitas,
                       SUM(DISPENSED_QTY_TOTAL) AS medicamentos,
                       COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS medicos
                FROM prescricoes
                GROUP BY STATE_DESC
                ORDER BY SUM(RX_COUNT_TOTAL) DESC
                LIMIT 20
            """)

            dash_data = {
                "kpis": kpis, "evolucao": evolucao,
                "share": share, "geo": geo,
                "generated_at": now_utc,
            }
            cache_set("dash::():MANUFACTURER_DESC:STATE_DESC", dash_data)
            os.makedirs(os.path.dirname(_STATIC_DASH), exist_ok=True)
            with open(_STATIC_DASH, "w", encoding="utf-8") as f:
                json.dump(dash_data, f, ensure_ascii=False, default=str)
            report["dados"]["dashboard"] = {
                "periodos": len(evolucao), "labs": len(share), "estados": len(geo)
            }

            # Ranking
            ranking = query("""
                SELECT DOCTOR_DISPLAY_CD                             AS crm,
                       TRIM(FIRST_NM) || ' ' || TRIM(SURNM_NM)     AS medico,
                       CITY_DESC AS cidade, STATE_DESC AS estado, IMS_BRICK_DESC AS brick,
                       SUM(RX_COUNT_TOTAL)                          AS total_receitas,
                       SUM(DISPENSED_QTY_TOTAL)                     AS total_medicamentos,
                       COUNT(DISTINCT MANUFACTURER_DESC)             AS qtde_labs,
                       COUNT(DISTINCT BRAND_NAME)                   AS qtde_marcas
                FROM prescricoes
                GROUP BY DOCTOR_DISPLAY_CD, FIRST_NM, SURNM_NM, CITY_DESC, STATE_DESC, IMS_BRICK_DESC
                ORDER BY SUM(RX_COUNT_TOTAL) DESC
                LIMIT 200
            """)
            cache_set("ranking::():200", ranking)
            rank_data = {"ranking": ranking, "generated_at": now_utc}
            with open(_STATIC_RANK, "w", encoding="utf-8") as f:
                json.dump(rank_data, f, ensure_ascii=False, default=str)
            report["dados"]["ranking"] = {"medicos": len(ranking)}

            report["ok"] = True
            report["generated_at"] = now_utc
        except Exception as e:
            report["erros"].append(str(e))
        result_q.put(report)

    t = threading.Thread(target=_gen, daemon=True)
    t.start()
    t.join(timeout=300)   # espera até 5 min

    if result_q.empty():
        return jsonify({"ok": False, "erro": "Timeout ao gerar dados (>5 min)"}), 504
    return jsonify(result_q.get())

# ── Prescritores ──────────────────────────────────────────────────────────
@app.route("/api/prescritores/ranking")
@login_required
def prescritores_ranking():
    # Arquivo gerado → serve direto, sem nenhuma query
    if os.path.exists(_STATIC_RANK):
        with open(_STATIC_RANK, encoding="utf-8-sig") as f:  # utf-8-sig strips BOM gerado pelo PowerShell
            data = json.load(f)
        # ranking.json tem formato {"ranking": [...], "generated_at": "..."}
        rows = data.get("ranking", data) if isinstance(data, dict) else data
        return jsonify(rows)

    # Sem arquivo → busca ao vivo no SAP IQ
    try:
        rows = query("""
            SELECT DOCTOR_DISPLAY_CD                             AS crm,
                   TRIM(FIRST_NM) || ' ' || TRIM(SURNM_NM)     AS medico,
                   CITY_DESC AS cidade, STATE_DESC AS estado, IMS_BRICK_DESC AS brick,
                   SUM(RX_COUNT_TOTAL)                          AS total_receitas,
                   SUM(DISPENSED_QTY_TOTAL)                     AS total_medicamentos,
                   COUNT(DISTINCT MANUFACTURER_DESC)             AS qtde_labs,
                   COUNT(DISTINCT BRAND_NAME)                   AS qtde_marcas
            FROM prescricoes
            GROUP BY DOCTOR_DISPLAY_CD, FIRST_NM, SURNM_NM, CITY_DESC, STATE_DESC, IMS_BRICK_DESC
            ORDER BY SUM(RX_COUNT_TOTAL) DESC LIMIT 200
        """)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"erro": str(e)}), 503

@app.route("/api/prescritores/perfil/<crm_id>")
@login_required
def prescritor_perfil(crm_id):
    info = query("""
        SELECT DOCTOR_DISPLAY_CD                             AS crm,
               TRIM(FIRST_NM) || ' ' || TRIM(SURNM_NM)     AS medico,
               CITY_DESC                                     AS cidade,
               STATE_DESC                                    AS estado,
               IMS_BRICK_DESC                                AS brick,
               SUM(RX_COUNT_TOTAL)                          AS total_receitas,
               SUM(DISPENSED_QTY_TOTAL)                     AS total_medicamentos
        FROM prescricoes WHERE DOCTOR_DISPLAY_CD=?
        GROUP BY DOCTOR_DISPLAY_CD, FIRST_NM, SURNM_NM, CITY_DESC, STATE_DESC, IMS_BRICK_DESC
    """, (crm_id,))
    prescricoes_det = query("""
        SELECT MANUFACTURER_DESC      AS laboratorio,
               BRAND_NAME             AS marca,
               COMBINED_MOLECULE_DESC AS molecula,
               PERIOD_CD              AS periodo,
               SUM(RX_COUNT_TOTAL)    AS receitas,
               SUM(DISPENSED_QTY_TOTAL) AS medicamentos
        FROM prescricoes WHERE DOCTOR_DISPLAY_CD=?
        GROUP BY MANUFACTURER_DESC, BRAND_NAME, COMBINED_MOLECULE_DESC, PERIOD_CD
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
    """, (crm_id,))
    return jsonify({"info": info[0] if info else {}, "prescricoes": prescricoes_det})

@app.route("/api/prescritores/oportunidades")
@login_required
def prescritores_oportunidades():
    molecula    = request.args.get("molecula", "")
    laboratorio = request.args.get("laboratorio", "")
    if not molecula or not laboratorio:
        return jsonify({"error": "Informe molecula e laboratorio"}), 400
    estado  = request.args.get("estado", "")
    cidade  = request.args.get("cidade", "")
    extra_clauses, extra_params = [], []
    if estado:
        extra_clauses.append("STATE_DESC = ?"); extra_params.append(estado)
    if cidade:
        extra_clauses.append("CITY_DESC = ?"); extra_params.append(cidade)
    extra_w = ("AND " + " AND ".join(extra_clauses)) if extra_clauses else ""
    # Médicos que prescrevem a molécula mas NÃO prescrevem o laboratório alvo
    rows = query(f"""
        SELECT DOCTOR_DISPLAY_CD                              AS crm,
               TRIM(FIRST_NM) || ' ' || TRIM(SURNM_NM)      AS medico,
               CITY_DESC                                      AS cidade,
               STATE_DESC                                     AS estado,
               IMS_BRICK_DESC                                 AS brick,
               SUM(RX_COUNT_TOTAL)                           AS total_receitas,
               SUM(DISPENSED_QTY_TOTAL)                      AS total_medicamentos,
               COUNT(DISTINCT MANUFACTURER_DESC)              AS qtde_labs
        FROM prescricoes
        WHERE COMBINED_MOLECULE_DESC=? {extra_w}
          AND DOCTOR_DISPLAY_CD NOT IN (
              SELECT DISTINCT DOCTOR_DISPLAY_CD FROM prescricoes
              WHERE COMBINED_MOLECULE_DESC=? AND MANUFACTURER_DESC=?
          )
        GROUP BY DOCTOR_DISPLAY_CD, FIRST_NM, SURNM_NM, CITY_DESC, STATE_DESC, IMS_BRICK_DESC
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
        LIMIT 200
    """, (molecula,) + tuple(extra_params) + (molecula, laboratorio))
    return jsonify(rows)

# ── Geo: top moléculas da base (para sugestões dinâmicas) ─────────────────
@app.route("/api/geo/moleculas-top")
@login_required
def geo_moleculas_top():
    limit = min(int(request.args.get("limit", "12")), 30)
    try:
        rows = query(f"""
            SELECT TOP {limit}
                   COMBINED_MOLECULE_DESC AS molecula,
                   SUM(RX_COUNT_TOTAL)               AS total_receitas,
                   COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS total_medicos
            FROM prescricoes
            WHERE COMBINED_MOLECULE_DESC IS NOT NULL
              AND COMBINED_MOLECULE_DESC <> ''
            GROUP BY COMBINED_MOLECULE_DESC
            ORDER BY SUM(RX_COUNT_TOTAL) DESC
        """)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"erro": str(e)}), 503

# ── Geo: cidades com mais prescritores por medicamento/molécula ───────────
@app.route("/api/geo/prescritores-bairro")
@login_required
def geo_prescritores_bairro():
    """
    Retorna top localidades com mais médicos que prescrevem um medicamento/molécula.
    Parâmetros:
      q        — termo de busca (obrigatório)
      limit    — máx resultados (padrão 10, máx 20)
      groupby  — 'cidade' (padrão) ou 'brick'
    """
    q_raw   = request.args.get("q", "").strip()
    limit   = min(int(request.args.get("limit", "10")), 20)
    groupby = request.args.get("groupby", "cidade").lower()
    if groupby not in ("cidade", "brick"):
        groupby = "cidade"
    if not q_raw:
        return jsonify({"erro": "Parâmetro 'q' obrigatório"}), 400
    q_safe = re.sub(r"[^A-Za-z0-9À-ÿ\s\-]", "", q_raw).strip()
    if not q_safe:
        return jsonify({"erro": "Termo de busca inválido"}), 400
    like_val = f"%{q_safe.upper()}%"
    try:
        if groupby == "brick":
            rows = query(f"""
                SELECT TOP {limit}
                       IMS_BRICK_DESC AS brick,
                       CITY_DESC      AS cidade,
                       STATE_DESC     AS estado,
                       COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS total_medicos,
                       SUM(RX_COUNT_TOTAL)               AS total_receitas
                FROM prescricoes
                WHERE UPPER(BRAND_NAME) LIKE ?
                   OR UPPER(COMBINED_MOLECULE_DESC) LIKE ?
                GROUP BY IMS_BRICK_DESC, CITY_DESC, STATE_DESC
                ORDER BY COUNT(DISTINCT DOCTOR_DISPLAY_CD) DESC
            """, (like_val, like_val))
        else:
            rows = query(f"""
                SELECT TOP {limit}
                       CITY_DESC  AS cidade,
                       STATE_DESC AS estado,
                       COUNT(DISTINCT DOCTOR_DISPLAY_CD) AS total_medicos,
                       SUM(RX_COUNT_TOTAL)               AS total_receitas
                FROM prescricoes
                WHERE UPPER(BRAND_NAME) LIKE ?
                   OR UPPER(COMBINED_MOLECULE_DESC) LIKE ?
                GROUP BY CITY_DESC, STATE_DESC
                ORDER BY COUNT(DISTINCT DOCTOR_DISPLAY_CD) DESC
            """, (like_val, like_val))
        return jsonify({"termo": q_safe, "groupby": groupby, "resultados": rows})
    except Exception as e:
        return jsonify({"erro": str(e)}), 503

# ── Chat ──────────────────────────────────────────────────────────────────
def safe_sql(sql):
    s = sql.strip()
    s_up = s.upper().lstrip()
    if not s_up.startswith("SELECT"):
        raise ValueError("Apenas queries SELECT são permitidas.")
    for kw in ["DROP","DELETE","UPDATE","INSERT","CREATE","ALTER","ATTACH","DETACH","PRAGMA"]:
        if re.search(r'\b' + kw + r'\b', s_up):
            raise ValueError(f"Operação não permitida: {kw}")
    # Garante limite de linhas (LIMIT → TOP para SQL Server)
    if "LIMIT" in s_up:
        s = adapt_sql(s)
    elif "TOP" not in s_up:
        s = re.sub(r'^(\s*SELECT\s)', 'SELECT TOP 100 ', s, flags=re.IGNORECASE, count=1)
    return query(s)

@app.route("/api/chat", methods=["POST"])
@login_required
def chat():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY não configurada"}), 500
    payload = request.json
    hdrs = {"Content-Type": "application/json", "x-api-key": api_key, "anthropic-version": "2023-06-01"}
    query_tool = {
        "name": "query_database",
        "description": (
            f"Executa SQL SELECT na tabela {TABLE_PRESC} (Sybase IQ 16). "
            "Use TOP n em vez de LIMIT n. Use || para concatenar strings. "
            "Colunas disponíveis: "
            "DOCTOR_DISPLAY_CD (CRM do médico), "
            "FIRST_NM (primeiro nome do médico), SURNM_NM (sobrenome do médico), "
            "PERIOD_CD (período inteiro YYYYMM), "
            "CHANNEL_DESC (canal de venda), "
            "IMS_BRICK_DESC (brick geográfico IMS), "
            "CITY_DESC (cidade), STATE_DESC (estado), "
            "MANUFACTURER_DESC (laboratório/fabricante), "
            "BRAND_NAME (marca do produto), "
            "COMBINED_MOLECULE_DESC (molécula/princípio ativo), "
            "RX_COUNT_TOTAL (quantidade de receitas), "
            "DISPENSED_QTY_TOTAL (quantidade de medicamentos dispensados)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string"}},
            "required": ["sql"]
        }
    }
    msgs = list(payload.get("messages", []))
    call = {
        "model":      payload.get("model", "claude-haiku-4-5-20251001"),
        "max_tokens": payload.get("max_tokens", 1800),
        "system":     payload.get("system", ""),
        "tools":      [query_tool],
        "messages":   msgs
    }
    queries_run = []
    for _ in range(6):
        resp = requests.post("https://api.anthropic.com/v1/messages",
                             headers=hdrs, json=call, timeout=90)
        if resp.status_code != 200:
            return jsonify(resp.json()), resp.status_code
        data    = resp.json()
        stop    = data.get("stop_reason")
        content = data.get("content", [])
        if stop == "tool_use":
            call["messages"].append({"role": "assistant", "content": content})
            results = []
            for blk in content:
                if blk.get("type") == "tool_use" and blk.get("name") == "query_database":
                    sql = blk.get("input", {}).get("sql", "")
                    try:
                        rows = safe_sql(sql)
                        queries_run.append({"sql": sql, "linhas": len(rows)})
                        payload_res = json.dumps({"linhas": len(rows), "dados": rows},
                                                 ensure_ascii=False, default=str)
                    except Exception as e:
                        payload_res = json.dumps({"erro": str(e)})
                    results.append({"type": "tool_result", "tool_use_id": blk["id"], "content": payload_res})
            call["messages"].append({"role": "user", "content": results})
        else:
            if queries_run:
                data["queries_executed"] = queries_run
            return jsonify(data), resp.status_code
    return jsonify(data), 200

# ── Admin: carga de dados ─────────────────────────────────────────────────
ADMIN_KEY = os.environ.get("ADMIN_KEY", "iqvia-admin-2026")

@app.route("/api/status")
@login_required
def api_status():
    """Informa se os dados já estão prontos no cache."""
    dash_ok    = cache_get("dash::():MANUFACTURER_DESC:STATE_DESC") is not None
    ranking_ok = cache_get("ranking::():200") is not None
    return jsonify({
        "dashboard_pronto": dash_ok,
        "ranking_pronto":   ranking_ok,
        "pronto":           dash_ok and ranking_ok,
    })

@app.route("/admin/load", methods=["GET"])
@login_required
def admin_load_page():
    exists = table_exists("prescricoes")
    count  = query("SELECT COUNT(*) AS cnt FROM prescricoes")[0]["cnt"] if exists else 0
    return render_template("admin_load.html", table_exists=exists, row_count=count)

@app.route("/admin/load", methods=["POST"])
@login_required
def admin_load_post():
    key = request.form.get("admin_key", "")
    if key != ADMIN_KEY:
        return render_template("admin_load.html", error="Chave de admin incorreta.",
                               table_exists=False, row_count=0)

    f = request.files.get("csv_file")
    if not f or not f.filename.lower().endswith(".csv"):
        return render_template("admin_load.html", error="Envie um arquivo .csv válido.",
                               table_exists=False, row_count=0)

    tmp_path = os.path.join(os.path.dirname(__file__), "data", "_upload_tmp.csv")
    try:
        f.save(tmp_path)
        loaded = 0
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(tmp_path, skiprows=1, encoding=enc, header=0)
                break
            except UnicodeDecodeError:
                continue

        df.columns = ["crm","medico","periodo","canal","brick","cidade","estado",
                      "laboratorio","marca","molecula","qtde_med","qtde_rec"]
        df["molecula"] = df["molecula"].str.strip()
        df["periodo"]  = df["periodo"].astype(str)
        df["qtde_med"] = pd.to_numeric(df["qtde_med"], errors="coerce").fillna(0).astype(int)
        df["qtde_rec"] = pd.to_numeric(df["qtde_rec"], errors="coerce").fillna(0).astype(int)
        loaded = len(df)
        _df_to_table(df, "prescricoes")
    except Exception as e:
        return render_template("admin_load.html", error=f"Erro ao processar: {e}",
                               table_exists=False, row_count=0)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    cache_clear()
    ensure_indexes()
    return render_template("admin_load.html", success=True, row_count=loaded,
                           table_exists=True)

# ── Debug ────────────────────────────────────────────────────────────────
@app.route("/api/debug")
def debug():
    result = {
        "use_http_api": USE_HTTP_API,
        "agent_url":    _AGENT_URL,
        "table":        TABLE_PRESC,
        "table_prescricoes": False,
        "row_count": 0,
        "error": None
    }
    try:
        result["table_prescricoes"] = table_exists("prescricoes")
        if result["table_prescricoes"]:
            r = query("SELECT COUNT(*) AS cnt FROM prescricoes")
            result["row_count"] = r[0]["cnt"]
    except Exception as e:
        result["error"] = str(e)
    return jsonify(result)

@app.route("/api/test-db")
def test_db():
    """Testa conexão com o Java Agent via Cloudflare Tunnel."""
    import time
    result = {
        "config": {
            "agent_url":     _AGENT_URL,
            "api_key_set":   bool(_AGENT_API_KEY),
            "table":         TABLE_PRESC,
        },
        "steps": {}
    }

    # 1. Health check do agent
    t0 = time.time()
    try:
        hdrs = {"X-API-Key": _AGENT_API_KEY} if _AGENT_API_KEY else {}
        h = requests.get(f"{_AGENT_URL}/health", headers=hdrs, verify=True, timeout=10)
        result["steps"]["1_health"] = {"ok": h.status_code == 200, "status": h.status_code,
                                       "resposta": h.json() if h.headers.get("content-type","").startswith("application/json") else h.text,
                                       "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["1_health"] = {"ok": False, "erro": str(e), "ms": round((time.time()-t0)*1000)}

    # 2. Ping via SELECT 1
    t0 = time.time()
    try:
        rows = query("SELECT 1 AS ping")
        result["steps"]["2_ping"] = {"ok": True, "resposta": rows, "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["2_ping"] = {"ok": False, "erro": str(e), "ms": round((time.time()-t0)*1000)}
        result["status"] = "FALHOU"
        return jsonify(result)

    # 2. Tabela existe?
    t0 = time.time()
    try:
        existe = table_exists("prescricoes")
        result["steps"]["3_tabela"] = {"ok": existe, "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["3_tabela"] = {"ok": False, "erro": str(e)}

    # 4. Contagem de linhas
    t0 = time.time()
    try:
        r = query(f"SELECT COUNT(*) AS cnt FROM {TABLE_PRESC}")
        cnt = list(r[0].values())[0] if r else 0
        result["steps"]["4_contagem"] = {"ok": True, "total_linhas": cnt, "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["4_contagem"] = {"ok": False, "erro": str(e)}

    # 5. Colunas da tabela via /schema
    t0 = time.time()
    try:
        hdrs = {"X-API-Key": _AGENT_API_KEY} if _AGENT_API_KEY else {}
        sr = requests.get(f"{_AGENT_URL}/schema/PBS_AI_ANALYTICS",
                          headers=hdrs, verify=True, timeout=15)
        result["steps"]["5_schema"] = {"ok": sr.status_code == 200,
                                       "colunas": sr.json() if sr.ok else sr.text,
                                       "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["5_schema"] = {"ok": False, "erro": str(e)}

    result["status"] = "OK" if all(v.get("ok") for v in result["steps"].values()) else "PARCIAL"
    return jsonify(result)

# ── Pages ─────────────────────────────────────────────────────────────────
@app.route("/")
@login_required
def index():
    return redirect(url_for("market_page"))

@app.route("/market")
@login_required
def market_page():
    return render_template("market.html")

@app.route("/prescritores")
@login_required
def prescritores_page():
    return render_template("prescritores.html")

@app.route("/chat")
@login_required
def chat_page():
    return render_template("chat.html")

@app.route("/mapa")
@login_required
def mapa_page():
    return render_template("mapa.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
