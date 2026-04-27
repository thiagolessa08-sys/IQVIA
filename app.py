from flask import Flask, jsonify, render_template, request, session, redirect, url_for
from functools import wraps
import sqlite3, os, requests, json, re
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "iqvia-pharma-2026-xK9m")
app.config["MAX_CONTENT_LENGTH"] = 300 * 1024 * 1024  # 300 MB
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "iqvia.db")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_POSTGRES = bool(DATABASE_URL)

# ── DB Layer (SQLAlchemy + pg8000, sem dependência de libpq) ──────────────
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        url = DATABASE_URL
        url = url.replace("postgres://", "postgresql+pg8000://", 1)
        url = url.replace("postgresql://", "postgresql+pg8000://", 1)
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine

def _to_named(sql, params):
    named_sql, named_params = sql, {}
    for i, p in enumerate(params):
        named_sql = named_sql.replace("?", f":p{i}", 1)
        named_params[f"p{i}"] = p
    return named_sql, named_params

def query(sql, params=()):
    if USE_POSTGRES:
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
    if USE_POSTGRES:
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
    if USE_POSTGRES:
        rows = query("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name=?) AS ex", (name,))
        return bool(rows[0]["ex"])
    rows = query("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return bool(rows)

def _df_to_table(df, table_name):
    if USE_POSTGRES:
        df.to_sql(table_name, get_engine(), if_exists="replace", index=False, chunksize=5000)
    else:
        con = sqlite3.connect(DB_PATH)
        df.to_sql(table_name, con, if_exists="replace", index=False)
        con.close()

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

try:
    init_prescricoes()
except Exception as e:
    print(f"[init] Aviso: {e}")

# ── Helpers de filtro ─────────────────────────────────────────────────────
def build_filters(args):
    clauses, params = [], []
    if args.get("molecula"):
        clauses.append("molecula = ?"); params.append(args["molecula"].strip())
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
@app.route("/api/filters/moleculas")
@login_required
def filter_moleculas():
    rows = query("SELECT DISTINCT molecula FROM prescricoes ORDER BY molecula")
    return jsonify([r["molecula"] for r in rows if r["molecula"]])

@app.route("/api/filters/laboratorios")
@login_required
def filter_laboratorios():
    mol = request.args.get("molecula", "")
    if mol:
        rows = query("SELECT DISTINCT laboratorio FROM prescricoes WHERE molecula=? ORDER BY laboratorio", (mol,))
    else:
        rows = query("SELECT DISTINCT laboratorio FROM prescricoes ORDER BY laboratorio")
    return jsonify([r["laboratorio"] for r in rows if r["laboratorio"]])

@app.route("/api/filters/estados")
@login_required
def filter_estados():
    rows = query("SELECT DISTINCT estado FROM prescricoes ORDER BY estado")
    return jsonify([r["estado"] for r in rows if r["estado"]])

@app.route("/api/filters/periodos")
@login_required
def filter_periodos():
    rows = query("SELECT DISTINCT periodo FROM prescricoes ORDER BY periodo")
    return jsonify([r["periodo"] for r in rows if r["periodo"]])

@app.route("/api/filters/cidades")
@login_required
def filter_cidades():
    estado = request.args.get("estado", "")
    if estado:
        rows = query("SELECT DISTINCT cidade FROM prescricoes WHERE estado=? ORDER BY cidade", (estado,))
    else:
        rows = query("SELECT DISTINCT cidade FROM prescricoes ORDER BY cidade")
    return jsonify([r["cidade"] for r in rows if r["cidade"]])

# ── Market Intelligence ───────────────────────────────────────────────────
@app.route("/api/market/kpis")
@login_required
def market_kpis():
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
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
    return jsonify(r[0])

@app.route("/api/market/share")
@login_required
def market_share():
    group_by = request.args.get("group_by", "laboratorio")
    if group_by not in ("laboratorio", "marca", "molecula"):
        group_by = "laboratorio"
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    total_q = query(f"SELECT COALESCE(SUM(qtde_rec), 1) AS total FROM prescricoes {w}", params)
    total = total_q[0]["total"] or 1
    rows = query(f"""
        SELECT {group_by} AS nome,
               SUM(qtde_rec)           AS receitas,
               SUM(qtde_med)           AS medicamentos,
               COUNT(DISTINCT crm)     AS medicos
        FROM prescricoes {w}
        GROUP BY {group_by}
        ORDER BY receitas DESC
        LIMIT 15
    """, params)
    for r in rows:
        r["share"] = round((r["receitas"] / total) * 100, 2)
    return jsonify(rows)

@app.route("/api/market/evolucao")
@login_required
def market_evolucao():
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    rows = query(f"""
        SELECT periodo,
               SUM(qtde_rec)  AS receitas,
               SUM(qtde_med)  AS medicamentos
        FROM prescricoes {w}
        GROUP BY periodo
        ORDER BY periodo
    """, params)
    return jsonify(rows)

@app.route("/api/market/geografico")
@login_required
def market_geografico():
    group_by = request.args.get("group_by", "estado")
    if group_by not in ("estado", "cidade", "brick"):
        group_by = "estado"
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
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
    return jsonify(rows)

# ── Prescritores ──────────────────────────────────────────────────────────
@app.route("/api/prescritores/ranking")
@login_required
def prescritores_ranking():
    filters, params = build_filters(request.args)
    w = f"WHERE {filters}" if filters else ""
    limit = min(request.args.get("limit", 200, type=int), 500)
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
    if "LIMIT" not in s_up:
        s = s.rstrip(";") + " LIMIT 100"
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
            "Executa SQL SELECT na tabela prescricoes. "
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

    return render_template("admin_load.html", success=True, row_count=loaded,
                           table_exists=True)

# ── Debug ────────────────────────────────────────────────────────────────
@app.route("/api/debug")
def debug():
    result = {
        "use_postgres": USE_POSTGRES,
        "database_url_set": bool(os.environ.get("DATABASE_URL")),
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
