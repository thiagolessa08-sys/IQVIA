from flask import Flask, jsonify, render_template, request, session, redirect, url_for
from functools import wraps
import sqlite3, os, requests, json, re, time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "iqvia-pharma-2026-xK9m")
app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024  # 300 MB
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "iqvia.db")

_MSSQL_HOST = os.environ.get("MSSQL_HOST", "claude.sqltech.com.br")
_MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "2626"))
_MSSQL_USER = os.environ.get("MSSQL_USER", "iaapi")
_MSSQL_PASS = os.environ.get("MSSQL_PASS", "i@sql2025HML")
_MSSQL_DB   = os.environ.get("MSSQL_DB",   "IQHML")
USE_MSSQL   = bool(_MSSQL_HOST and _MSSQL_USER)

# ── DB Layer (SQLAlchemy + pymssql — SQL Server) ──────────────────────────
_engine      = None
_CA_CERT_PATH = None   # preenchido por _setup_mssql_ssl()

def _setup_mssql_ssl():
    """Decodifica o PFX (env MSSQL_CERT_B64) e configura FreeTDS para SSL."""
    global _CA_CERT_PATH
    cert_b64  = os.environ.get("MSSQL_CERT_B64", "")
    cert_pass = os.environ.get("MSSQL_CERT_PASS", "1234").encode()
    if not cert_b64:
        return
    try:
        import base64
        from cryptography.hazmat.primitives.serialization import pkcs12, Encoding
        pfx_data = base64.b64decode(cert_b64)
        _, certificate, extra = pkcs12.load_key_and_certificates(pfx_data, cert_pass)
        # Grava cadeia de certificados em PEM
        ca_path = "/tmp/sqltech_ca.pem"
        with open(ca_path, "wb") as f:
            if certificate:
                f.write(certificate.public_bytes(Encoding.PEM))
            for c in (extra or []):
                f.write(c.public_bytes(Encoding.PEM))
        # Cria freetds.conf apontando para o cert
        freetds_conf = (
            f"[{_MSSQL_HOST}]\n"
            f"    host = {_MSSQL_HOST}\n"
            f"    port = {_MSSQL_PORT}\n"
            f"    tds version = 7.4\n"
            f"    ssl = yes\n"
            f"    ca file = {ca_path}\n"
        )
        freetds_path = "/tmp/freetds.conf"
        with open(freetds_path, "w") as f:
            f.write(freetds_conf)
        os.environ["FREETDSCONF"] = freetds_path
        _CA_CERT_PATH = ca_path
        print(f"[ssl] Certificado configurado em {ca_path}")
    except Exception as e:
        print(f"[ssl] Aviso ao configurar SSL: {e}")

_setup_mssql_ssl()   # roda uma vez no startup

def get_engine():
    global _engine
    if _engine is None:
        from urllib.parse import quote_plus
        pw  = quote_plus(_MSSQL_PASS)
        url = (f"mssql+pymssql://{_MSSQL_USER}:{pw}"
               f"@{_MSSQL_HOST}:{_MSSQL_PORT}/{_MSSQL_DB}")
        _engine = create_engine(
            url,
            pool_pre_ping=False,
            pool_size=5,
            max_overflow=10,
            connect_args={"timeout": 8},
        )
    return _engine

# Tabela real no banco SQL Server
TABLE_PRESC = os.environ.get("TABLE_PRESC", "qqhetl.PBS_AI_ANALYTICS")

def adapt_sql(sql):
    """Redireciona 'prescricoes' para a tabela real e converte LIMIT→TOP."""
    sql = re.sub(r'\bprescricoes\b', TABLE_PRESC, sql)
    m = re.search(r'\bLIMIT\s+(\d+)\s*;?\s*$', sql.strip(), re.IGNORECASE)
    if m:
        n   = m.group(1)
        sql = re.sub(r'\bLIMIT\s+\d+\s*;?\s*$', '', sql.strip(), flags=re.IGNORECASE).rstrip()
        sql = re.sub(r'^(\s*SELECT\s)', f'SELECT TOP {n} ', sql, flags=re.IGNORECASE, count=1)
    return sql

def _to_named(sql, params):
    named_sql, named_params = sql, {}
    for i, p in enumerate(params):
        named_sql = named_sql.replace("?", f":p{i}", 1)
        named_params[f"p{i}"] = p
    return named_sql, named_params

def query(sql, params=()):
    if USE_MSSQL:
        sql = adapt_sql(sql)
        named_sql, named_params = _to_named(sql, params)
        with get_engine().connect() as conn:
            result = conn.execute(text(named_sql), named_params)
            return [dict(r._mapping) for r in result.fetchall()]
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def execute(sql, params=()):
    if USE_MSSQL:
        sql = adapt_sql(sql)
        named_sql, named_params = _to_named(sql, params)
        with get_engine().connect() as conn:
            conn.execute(text(named_sql), named_params)
            conn.commit()
        return
    con = sqlite3.connect(DB_PATH)
    con.execute(sql, params)
    con.commit()
    con.close()

def table_exists(name):
    if USE_MSSQL:
        # Verifica a tabela real (ignora o argumento 'name' no modo MSSQL)
        rows = query(
            "SELECT COUNT(*) AS ex FROM information_schema.tables "
            "WHERE table_schema='qqhetl' AND table_name='PBS_AI_ANALYTICS'")
        return bool(rows[0]["ex"])
    rows = query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return bool(rows)

def _df_to_table(df, table_name):
    if USE_MSSQL:
        df.to_sql("PBS_AI_ANALYTICS", get_engine(), schema="qqhetl",
                  if_exists="replace", index=False, chunksize=5000)
    else:
        con = sqlite3.connect(DB_PATH)
        df.to_sql(table_name, con, if_exists="replace", index=False)
        con.close()

# ── Cache simples em memória (TTL) ───────────────────────────────────────
_cache = {}
CACHE_TTL = 120  # segundos

def cache_get(key):
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL:
        return entry["data"]
    return None

def cache_set(key, data):
    _cache[key] = {"data": data, "ts": time.time()}

def cache_clear():
    _cache.clear()

# ── Auth ──────────────────────────────────────────────────────────────────
USERS = {
    "admin@iqvia.com":              {"password": "Iqvia2026",   "name": "Admin IQVIA"},
    "marcio.amorim@sqltech.com.br": {"password": "Sqltech123",  "name": "Márcio Amorim"},
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
    if USE_MSSQL:
        print(f"[init] Modo SQL Server — usando tabela {TABLE_PRESC}.")
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
    if not USE_MSSQL:
        return
    T = TABLE_PRESC
    idxs = [
        ("idx_pbs_molecula",    f"{T}(molecula)"),
        ("idx_pbs_laboratorio", f"{T}(laboratorio)"),
        ("idx_pbs_estado",      f"{T}(estado)"),
        ("idx_pbs_periodo",     f"{T}(periodo)"),
        ("idx_pbs_crm",         f"{T}(crm)"),
        ("idx_pbs_marca",       f"{T}(marca)"),
    ]
    for name, cols in idxs:
        try:
            with get_engine().connect() as conn:
                exists = conn.execute(text(
                    "SELECT 1 FROM sys.indexes "
                    f"WHERE name=:n AND object_id=OBJECT_ID('{T}')"
                ), {"n": name}).fetchone()
                if not exists:
                    conn.execute(text(f"CREATE INDEX {name} ON {cols}"))
                    conn.commit()
        except Exception:
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
        clauses.append(f"molecula IN ({placeholders})")
        params.extend(mols)
    if args.get("laboratorio"):
        clauses.append("laboratorio = ?"); params.append(args["laboratorio"])
    if args.get("estado"):
        clauses.append("estado = ?"); params.append(args["estado"])
    if args.get("cidade"):
        clauses.append("cidade = ?"); params.append(args["cidade"])
    if args.get("brick"):
        clauses.append("brick = ?"); params.append(args["brick"])
    if args.get("periodo_ini"):
        clauses.append("periodo >= ?"); params.append(args["periodo_ini"])
    if args.get("periodo_fim"):
        clauses.append("periodo <= ?"); params.append(args["periodo_fim"])
    return " AND ".join(clauses), tuple(params)

# ── Filtros disponíveis ───────────────────────────────────────────────────
@app.route("/api/filters/all")
@login_required
def filter_all():
    """Retorna todos os filtros em uma única chamada."""
    cached = cache_get("filters_all")
    if cached:
        return jsonify(cached)
    mols     = query("SELECT DISTINCT molecula    FROM prescricoes WHERE molecula    IS NOT NULL ORDER BY molecula")
    labs     = query("SELECT DISTINCT laboratorio FROM prescricoes WHERE laboratorio IS NOT NULL ORDER BY laboratorio")
    estados  = query("SELECT DISTINCT estado      FROM prescricoes WHERE estado      IS NOT NULL ORDER BY estado")
    periodos = query("SELECT DISTINCT periodo     FROM prescricoes WHERE periodo     IS NOT NULL ORDER BY periodo")
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
    rows = query("SELECT DISTINCT molecula FROM prescricoes WHERE molecula IS NOT NULL ORDER BY molecula")
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
        rows = query(f"SELECT DISTINCT laboratorio FROM prescricoes WHERE molecula IN ({placeholders}) AND laboratorio IS NOT NULL ORDER BY laboratorio", tuple(mols))
        return jsonify([r["laboratorio"] for r in rows])
    cached = cache_get("filter_laboratorios")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT laboratorio FROM prescricoes WHERE laboratorio IS NOT NULL ORDER BY laboratorio")
    data = [r["laboratorio"] for r in rows]
    cache_set("filter_laboratorios", data)
    return jsonify(data)

@app.route("/api/filters/estados")
@login_required
def filter_estados():
    cached = cache_get("filter_estados")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT estado FROM prescricoes WHERE estado IS NOT NULL ORDER BY estado")
    data = [r["estado"] for r in rows]
    cache_set("filter_estados", data)
    return jsonify(data)

@app.route("/api/filters/periodos")
@login_required
def filter_periodos():
    cached = cache_get("filter_periodos")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT periodo FROM prescricoes WHERE periodo IS NOT NULL ORDER BY periodo")
    data = [r["periodo"] for r in rows]
    cache_set("filter_periodos", data)
    return jsonify(data)

@app.route("/api/filters/cidades")
@login_required
def filter_cidades():
    estado = request.args.get("estado", "")
    if estado:
        rows = query("SELECT DISTINCT cidade FROM prescricoes WHERE estado=? AND cidade IS NOT NULL ORDER BY cidade", (estado,))
        return jsonify([r["cidade"] for r in rows])
    cached = cache_get("filter_cidades")
    if cached: return jsonify(cached)
    rows = query("SELECT DISTINCT cidade FROM prescricoes WHERE cidade IS NOT NULL ORDER BY cidade")
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
            COALESCE(SUM(qtde_rec), 0)       AS total_receitas,
            COALESCE(SUM(qtde_med), 0)       AS total_medicamentos,
            COUNT(DISTINCT crm)              AS qtde_medicos,
            COUNT(DISTINCT laboratorio)      AS qtde_laboratorios,
            COUNT(DISTINCT marca)            AS qtde_marcas,
            COUNT(DISTINCT molecula)         AS qtde_moleculas
        FROM prescricoes {w}
    """, params)
    cache_set(ck, r[0])
    return jsonify(r[0])

@app.route("/api/market/share")
@login_required
def market_share():
    group_by = request.args.get("group_by", "laboratorio")
    if group_by not in ("laboratorio", "marca", "molecula"):
        group_by = "laboratorio"
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    ck = f"share:{group_by}:{filters}:{params}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    rows = query(f"""
        SELECT {group_by} AS nome,
               SUM(qtde_rec)                                        AS receitas,
               SUM(qtde_med)                                        AS medicamentos,
               COUNT(DISTINCT crm)                                  AS medicos,
               ROUND(SUM(qtde_rec) * 100.0 / SUM(SUM(qtde_rec)) OVER (), 2) AS share
        FROM prescricoes {w}
        GROUP BY {group_by}
        ORDER BY receitas DESC
        LIMIT 15
    """, params)
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
        SELECT periodo,
               SUM(qtde_rec)  AS receitas,
               SUM(qtde_med)  AS medicamentos
        FROM prescricoes {w}
        GROUP BY periodo
        ORDER BY periodo
    """, params)
    cache_set(ck, rows)
    return jsonify(rows)

@app.route("/api/market/geografico")
@login_required
def market_geografico():
    group_by = request.args.get("group_by", "estado")
    if group_by not in ("estado", "cidade", "brick"):
        group_by = "estado"
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    ck = f"geo:{group_by}:{filters}:{params}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    rows = query(f"""
        SELECT {group_by}            AS regiao,
               SUM(qtde_rec)         AS receitas,
               SUM(qtde_med)         AS medicamentos,
               COUNT(DISTINCT crm)   AS medicos
        FROM prescricoes {w}
        GROUP BY {group_by}
        ORDER BY receitas DESC
        LIMIT 20
    """, params)
    cache_set(ck, rows)
    return jsonify(rows)

# ── Prescritores ──────────────────────────────────────────────────────────
@app.route("/api/prescritores/ranking")
@login_required
def prescritores_ranking():
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    limit = min(request.args.get("limit", 200, type=int), 500)
    ck = f"ranking:{filters}:{params}:{limit}"
    cached = cache_get(ck)
    if cached: return jsonify(cached)
    rows = query(f"""
        SELECT crm, medico, cidade, estado, brick,
               SUM(qtde_rec)              AS total_receitas,
               SUM(qtde_med)              AS total_medicamentos,
               COUNT(DISTINCT laboratorio) AS qtde_labs,
               COUNT(DISTINCT marca)       AS qtde_marcas
        FROM prescricoes {w}
        GROUP BY crm, medico, cidade, estado, brick
        ORDER BY total_receitas DESC
        LIMIT {limit}
    """, params)
    cache_set(ck, rows)
    return jsonify(rows)

@app.route("/api/prescritores/perfil/<crm_id>")
@login_required
def prescritor_perfil(crm_id):
    info = query("""
        SELECT crm, medico, cidade, estado, brick,
               SUM(qtde_rec) AS total_receitas,
               SUM(qtde_med) AS total_medicamentos
        FROM prescricoes WHERE crm=?
        GROUP BY crm, medico, cidade, estado, brick
    """, (crm_id,))
    prescricoes = query("""
        SELECT laboratorio, marca, molecula, periodo,
               SUM(qtde_rec) AS receitas, SUM(qtde_med) AS medicamentos
        FROM prescricoes WHERE crm=?
        GROUP BY laboratorio, marca, molecula, periodo
        ORDER BY receitas DESC
    """, (crm_id,))
    return jsonify({"info": info[0] if info else {}, "prescricoes": prescricoes})

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
        extra_clauses.append("estado = ?"); extra_params.append(estado)
    if cidade:
        extra_clauses.append("cidade = ?"); extra_params.append(cidade)
    extra_w = ("AND " + " AND ".join(extra_clauses)) if extra_clauses else ""
    # Médicos que prescrevem a molécula mas NÃO prescrevem o laboratório alvo
    rows = query(f"""
        SELECT crm, medico, cidade, estado, brick,
               SUM(qtde_rec) AS total_receitas,
               SUM(qtde_med) AS total_medicamentos,
               COUNT(DISTINCT laboratorio) AS qtde_labs
        FROM prescricoes
        WHERE molecula=? {extra_w}
          AND crm NOT IN (
              SELECT DISTINCT crm FROM prescricoes
              WHERE molecula=? AND laboratorio=?
          )
        GROUP BY crm, medico, cidade, estado, brick
        ORDER BY total_receitas DESC
        LIMIT 200
    """, (molecula,) + tuple(extra_params) + (molecula, laboratorio))
    return jsonify(rows)

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
            f"Executa SQL SELECT na tabela {TABLE_PRESC} (SQL Server). "
            "Use TOP n em vez de LIMIT n. "
            "Colunas: crm, medico, periodo (YYYYMM), canal, brick, cidade, estado, "
            "laboratorio, marca, molecula, qtde_med (qtde medicamentos), qtde_rec (qtde receitas)."
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
        "use_mssql":   USE_MSSQL,
        "mssql_host":  _MSSQL_HOST,
        "mssql_port":  _MSSQL_PORT,
        "mssql_user":  _MSSQL_USER,
        "mssql_db":    _MSSQL_DB,
        "ssl_cert":    _CA_CERT_PATH or "não configurado",
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
    """Testa a conexão com o SQL Server e retorna diagnóstico completo."""
    import time
    result = {
        "config": {
            "host":     _MSSQL_HOST,
            "port":     _MSSQL_PORT,
            "database": _MSSQL_DB,
            "user":     _MSSQL_USER,
            "table":    TABLE_PRESC,
            "ssl_cert": _CA_CERT_PATH or "não configurado",
        },
        "steps": {}
    }

    # 1. Conexão
    t0 = time.time()
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        result["steps"]["1_conexao"] = {"ok": True, "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["1_conexao"] = {"ok": False, "erro": str(e), "ms": round((time.time()-t0)*1000)}
        return jsonify(result)

    # 2. Tabela existe?
    t0 = time.time()
    try:
        existe = table_exists("prescricoes")
        result["steps"]["2_tabela"] = {"ok": existe, "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["2_tabela"] = {"ok": False, "erro": str(e)}

    # 3. Contagem de linhas
    t0 = time.time()
    try:
        r = query(f"SELECT COUNT(*) AS cnt FROM {TABLE_PRESC}")
        result["steps"]["3_contagem"] = {"ok": True, "total_linhas": r[0]["cnt"], "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["3_contagem"] = {"ok": False, "erro": str(e)}

    # 4. Amostra de colunas (TOP 1)
    t0 = time.time()
    try:
        r = query(f"SELECT TOP 1 * FROM {TABLE_PRESC}")
        result["steps"]["4_amostra"] = {"ok": True, "colunas": list(r[0].keys()) if r else [], "ms": round((time.time()-t0)*1000)}
    except Exception as e:
        result["steps"]["4_amostra"] = {"ok": False, "erro": str(e)}

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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
