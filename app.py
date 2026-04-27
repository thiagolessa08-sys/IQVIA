from flask import Flask, jsonify, render_template, request, session, redirect, url_for
from functools import wraps
import sqlite3, os, requests, json, re, time
from datetime import datetime
import pandas as pd

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "sqltech-orcamento-2026-xK9m")
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "municipal.db")

# ── Credenciais ──────────────────────────────────────────────────────────
USERS = {
    "marcio.amorim@sqltech.com.br": {
        "password": "Sqltech123",
        "name": "Márcio Amorim"
    }
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
        return redirect(url_for("index"))
    error = None
    if request.method == "POST":
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = USERS.get(email)
        if user and user["password"] == password:
            session["logged_in"]  = True
            session["user_email"] = email
            session["user_name"]  = user["name"]
            return redirect(url_for("index"))
        error = "E-mail ou senha incorretos."
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    # Apaga bases temporárias (não-seed) do banco
    try:
        con = sqlite3.connect(DB_PATH)
        temp_bases = con.execute("SELECT name FROM _bases_catalog WHERE is_seed=0").fetchall()
        for (name,) in temp_bases:
            con.execute(f"DROP TABLE IF EXISTS base_{name}")
        con.execute("DELETE FROM _bases_catalog WHERE is_seed=0")
        con.commit()
        con.close()
    except Exception as e:
        print(f"[logout] Erro ao limpar bases temporárias: {e}")
    session.clear()
    return redirect(url_for("login_page"))

def query(sql, params=()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

# ── Bases de Dados ────────────────────────────────────────────────────────
SEEDS_DIR = os.path.join(os.path.dirname(__file__), "data", "seeds")

def init_catalog():
    con = sqlite3.connect(DB_PATH)
    con.execute('''CREATE TABLE IF NOT EXISTS _bases_catalog (
        name TEXT PRIMARY KEY,
        label TEXT,
        filename TEXT,
        rows INTEGER,
        cols TEXT,
        uploaded_at TEXT,
        is_seed INTEGER DEFAULT 0
    )''')
    # Adiciona coluna is_seed caso o banco já exista sem ela
    try:
        con.execute("ALTER TABLE _bases_catalog ADD COLUMN is_seed INTEGER DEFAULT 0")
    except Exception:
        pass
    con.commit()
    con.close()

def init_seeds():
    """Importa automaticamente todos os arquivos de data/seeds/ como bases permanentes."""
    if not os.path.isdir(SEEDS_DIR):
        return
    con = sqlite3.connect(DB_PATH)
    for filename in os.listdir(SEEDS_DIR):
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in ("csv", "xlsx", "xls"):
            continue
        name = re.sub(r"[^a-z0-9]", "_", filename.rsplit(".", 1)[0].lower())[:40]
        # Só importa se ainda não existir como seed
        existing = con.execute("SELECT name FROM _bases_catalog WHERE name=? AND is_seed=1", (name,)).fetchone()
        if existing:
            continue
        filepath = os.path.join(SEEDS_DIR, filename)
        try:
            df = pd.read_csv(filepath, encoding="utf-8", sep=None, engine="python") if ext == "csv" else pd.read_excel(filepath)
            df.to_sql(f"base_{name}", con, if_exists="replace", index=False)
            label = filename.rsplit(".", 1)[0].replace("_", " ").title()
            cols_info = json.dumps([{"col": c, "type": str(df[c].dtype)} for c in df.columns])
            con.execute("INSERT OR REPLACE INTO _bases_catalog VALUES (?,?,?,?,?,?,1)",
                (name, label, filename, len(df), cols_info, datetime.now().strftime("%Y-%m-%d %H:%M")))
            print(f"[seeds] Importado: {filename} ({len(df)} linhas)")
        except Exception as e:
            print(f"[seeds] Erro ao importar {filename}: {e}")
    con.commit()
    con.close()

init_catalog()
init_seeds()

@app.route("/api/bases")
@login_required
def bases_list():
    rows = query("SELECT name, label, filename, rows, cols, uploaded_at FROM _bases_catalog ORDER BY uploaded_at DESC")
    return jsonify(rows)

@app.route("/api/bases/upload", methods=["POST"])
@login_required
def bases_upload():
    f = request.files.get("file")
    label = request.form.get("label", "").strip()
    if not f or not f.filename:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
    filename = f.filename
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("csv", "xlsx", "xls"):
        return jsonify({"error": "Formato não suportado. Use CSV ou Excel (.csv, .xlsx, .xls)"}), 400
    name = re.sub(r"[^a-z0-9]", "_", filename.rsplit(".", 1)[0].lower())[:40]
    existing = query("SELECT name FROM _bases_catalog WHERE name=?", (name,))
    if existing:
        name = name[:36] + "_" + str(int(time.time()))[-3:]
    try:
        if ext == "csv":
            df = pd.read_csv(f, encoding="utf-8", sep=None, engine="python")
        else:
            df = pd.read_excel(f)
    except Exception as e:
        return jsonify({"error": f"Erro ao ler arquivo: {str(e)}"}), 400
    con = sqlite3.connect(DB_PATH)
    df.to_sql(f"base_{name}", con, if_exists="replace", index=False)
    cols_info = json.dumps([{"col": c, "type": str(df[c].dtype)} for c in df.columns])
    con.execute("INSERT OR REPLACE INTO _bases_catalog VALUES (?,?,?,?,?,?)",
        (name, label or filename, filename, len(df), cols_info, datetime.now().strftime("%Y-%m-%d %H:%M")))
    con.commit()
    con.close()
    return jsonify({"ok": True, "name": name, "rows": len(df), "cols": len(df.columns)})

@app.route("/api/bases/<name>/preview")
@login_required
def bases_preview(name):
    name = re.sub(r"[^a-z0-9_]", "_", name)
    try:
        rows = query(f"SELECT * FROM base_{name} LIMIT 10")
        return jsonify(rows)
    except Exception:
        return jsonify({"error": "Base não encontrada"}), 404

@app.route("/api/bases/<name>", methods=["DELETE"])
@login_required
def bases_delete(name):
    name = re.sub(r"[^a-z0-9_]", "_", name)
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(f"DROP TABLE IF EXISTS base_{name}")
        con.execute("DELETE FROM _bases_catalog WHERE name=?", (name,))
        con.commit()
        con.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/anos")
def anos():
    r = query('SELECT DISTINCT "Número Ano" AS ano FROM receita ORDER BY ano')
    return jsonify([row["ano"] for row in r])

@app.route("/api/kpis")
def kpis():
    ano = request.args.get("ano", 2024, type=int)
    rec = query('SELECT SUM("Valor Arrecadação Receita") AS arrecadado, SUM("Valor Projeto Receita") AS projetado FROM receita WHERE "Número Ano"=?', (ano,))
    desp = query('SELECT SUM("Valor Mês Empenhado") AS empenhado, SUM("Valor Mês Liquidado") AS liquidado, SUM("Valor Mês Pago") AS pago FROM despesa WHERE "Número Ano"=?', (ano,))
    return jsonify({**rec[0], **desp[0], "ano": ano})

@app.route("/api/receita-mensal")
def receita_mensal():
    ano = request.args.get("ano", 2024, type=int)
    rows = query('SELECT "Número Mês" AS mes, "Descrição Mês" AS nome, SUM("Valor Arrecadação Receita") AS total FROM receita WHERE "Número Ano"=? GROUP BY mes, nome ORDER BY mes', (ano,))
    return jsonify(rows)

@app.route("/api/despesa-funcao")
def despesa_funcao_dash():
    ano = request.args.get("ano", 2024, type=int)
    rows = query('SELECT "Descrição Função" AS funcao, SUM("Valor Mês Empenhado") AS empenhado, SUM("Valor Mês Pago") AS pago FROM despesa WHERE "Número Ano"=? GROUP BY funcao ORDER BY empenhado DESC NULLS LAST', (ano,))
    return jsonify(rows)

@app.route("/api/receita-categoria")
def receita_categoria_dash():
    ano = request.args.get("ano", 2024, type=int)
    rows = query('SELECT "Descrição Categoria Econômica Receita" AS categoria, SUM("Valor Arrecadação Receita") AS total FROM receita WHERE "Número Ano"=? GROUP BY categoria ORDER BY total DESC', (ano,))
    return jsonify(rows)

# TELA RECEITA
@app.route("/api/receita/kpis")
def receita_kpis():
    ano = request.args.get("ano", 2024, type=int)
    mes = request.args.get("mes", 0, type=int)
    w  = f'"Número Ano"={ano}' + (f' AND "Número Mês"={mes}' if mes else '')
    wp = f'"Número Ano"={ano-1}' + (f' AND "Número Mês"={mes}' if mes else '')
    wa = f'"Número Ano"={ano}' + (f' AND "Número Mês"<={mes}' if mes else '')
    wpa= f'"Número Ano"={ano-1}' + (f' AND "Número Mês"<={mes}' if mes else '')
    cur  = query(f'SELECT SUM("Valor Projeto Receita") as orcado, SUM("Valor Projeto Receita")+COALESCE(SUM("Valor Alteração Orçamentária Receita"),0) as orcado_atualizado, SUM("Valor Arrecadação Receita") as arrecadado FROM receita WHERE {w}')
    prev = query(f'SELECT SUM("Valor Arrecadação Receita") as arrecadado_prev FROM receita WHERE {wp}')
    acum = query(f'SELECT SUM("Valor Arrecadação Receita") as acumulado FROM receita WHERE {wa}')
    pacum= query(f'SELECT SUM("Valor Arrecadação Receita") as acumulado_prev FROM receita WHERE {wpa}')
    return jsonify({**cur[0], **prev[0], "acumulado": acum[0]["acumulado"], "acumulado_prev": pacum[0]["acumulado_prev"]})

@app.route("/api/receita/historico")
def receita_historico():
    rows = query('SELECT "Número Ano" AS ano, "Número Mês" AS mes, SUM("Valor Arrecadação Receita") AS total FROM receita GROUP BY ano, mes ORDER BY ano, mes')
    return jsonify(rows)

@app.route("/api/receita/origem")
def receita_origem():
    ano = request.args.get("ano", 2024, type=int)
    mes = request.args.get("mes", 0, type=int)
    w = f'"Número Ano"={ano}' + (f' AND "Número Mês"={mes}' if mes else '')
    rows = query(f'SELECT "Descrição Origem Receita" AS origem, SUM("Valor Arrecadação Receita") AS arrecadado FROM receita WHERE {w} GROUP BY origem ORDER BY arrecadado DESC')
    return jsonify(rows)

# TELA DESPESA
@app.route("/api/despesa/kpis")
def despesa_kpis():
    ano = request.args.get("ano", 2024, type=int)
    mes = request.args.get("mes", 0, type=int)
    w  = f'"Número Ano"={ano}' + (f' AND "Número Mês"={mes}' if mes else '')
    wp = f'"Número Ano"={ano-1}' + (f' AND "Número Mês"={mes}' if mes else '')
    cur  = query(f'SELECT SUM("Valor Dotação Inicial") as dot_inicial, SUM("Valor Dotação Inicial")+COALESCE(SUM("Valor Alteração Orçamentaria Despesa"),0) as dot_atualizada, SUM("Valor Mês Empenhado") as empenhado, SUM("Valor Mês Liquidado") as liquidado, SUM("Valor Mês Pago") as pago FROM despesa WHERE {w}')
    prev = query(f'SELECT SUM("Valor Dotação Inicial") as dot_inicial_prev, SUM("Valor Mês Empenhado") as empenhado_prev, SUM("Valor Mês Liquidado") as liquidado_prev, SUM("Valor Mês Pago") as pago_prev FROM despesa WHERE {wp}')
    return jsonify({**cur[0], **prev[0]})

@app.route("/api/despesa/secretaria")
def despesa_secretaria():
    ano = request.args.get("ano", 2024, type=int)
    rows = query(f'SELECT "Descrição Unidade Orçamentária" AS secretaria, SUM("Valor Dotação Inicial") AS dot_inicial, SUM("Valor Dotação Inicial")+COALESCE(SUM("Valor Alteração Orçamentaria Despesa"),0) AS dot_atualizada, SUM("Valor Mês Empenhado") AS empenhado, SUM("Valor Mês Liquidado") AS liquidado, SUM("Valor Mês Pago") AS pago FROM despesa WHERE "Número Ano"={ano} GROUP BY secretaria ORDER BY dot_atualizada DESC NULLS LAST')
    return jsonify(rows)

@app.route("/api/despesa/categoria")
def despesa_categoria():
    ano = request.args.get("ano", 2024, type=int)
    rows = query(f'SELECT "Descrição Categoria" AS categoria, SUM("Valor Dotação Inicial") AS dotacao FROM despesa WHERE "Número Ano"={ano} GROUP BY categoria ORDER BY dotacao DESC')
    return jsonify(rows)

@app.route("/api/despesa/modalidade")
def despesa_modalidade():
    ano = request.args.get("ano", 2024, type=int)
    rows = query(f'SELECT "Descrição Modalidade" AS modalidade, SUM("Valor Dotação Inicial") AS dotacao FROM despesa WHERE "Número Ano"={ano} GROUP BY modalidade ORDER BY dotacao DESC')
    return jsonify(rows)

@app.route("/api/despesa/elemento")
def despesa_elemento():
    ano = request.args.get("ano", 2024, type=int)
    rows = query(f'SELECT "Descrição Elemento Despesa" AS elemento, SUM("Valor Dotação Inicial") AS dotacao, SUM("Valor Mês Empenhado") AS empenhado FROM despesa WHERE "Número Ano"={ano} GROUP BY elemento ORDER BY dotacao DESC NULLS LAST LIMIT 10')
    return jsonify(rows)

def safe_sql(sql):
    """Valida e executa SQL apenas SELECT com segurança."""
    s = sql.strip()
    s_up = s.upper().lstrip()
    if not s_up.startswith("SELECT"):
        raise ValueError("Apenas queries SELECT são permitidas.")
    blocked = ["DROP","DELETE","UPDATE","INSERT","CREATE","ALTER","ATTACH","DETACH","PRAGMA"]
    for kw in blocked:
        if re.search(r'\b' + kw + r'\b', s_up):
            raise ValueError(f"Operação não permitida: {kw}")
    # Garante LIMIT
    if "LIMIT" not in s_up:
        s = s.rstrip(";") + " LIMIT 100"
    return query(s)

@app.route("/api/chat", methods=["POST"])
def chat():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY não configurada"}), 500

    payload = request.json
    hdrs = {"Content-Type": "application/json", "x-api-key": api_key, "anthropic-version": "2023-06-01"}

    # Ferramenta de consulta SQL
    query_tool = {
        "name": "query_database",
        "description": (
            "Executa uma query SQL SELECT no banco de dados municipal. "
            "Use para obter dados precisos, cruzar tabelas com JOIN e calcular agregações. "
            "Tabelas: receita, despesa, e bases adicionais importadas (prefixo base_). "
            "Retorna até 100 linhas."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string",
                        "description": "Query SQL SELECT com LIMIT. Ex: SELECT ... FROM receita JOIN base_x ON ... LIMIT 50"}
            },
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

    for _ in range(6):   # loop agentico — máx 6 iterações
        resp = requests.post("https://api.anthropic.com/v1/messages",
                             headers=hdrs, json=call, timeout=90)
        if resp.status_code != 200:
            return jsonify(resp.json()), resp.status_code

        data = resp.json()
        stop  = data.get("stop_reason")
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
                    results.append({
                        "type": "tool_result",
                        "tool_use_id": blk["id"],
                        "content": payload_res
                    })
            call["messages"].append({"role": "user", "content": results})
        else:
            if queries_run:
                data["queries_executed"] = queries_run
            return jsonify(data), resp.status_code

    return jsonify(data), 200

@app.route("/api/chat/context")
def chat_context():
    anos_rec  = query('SELECT DISTINCT "Número Ano" AS ano FROM receita ORDER BY ano')
    anos_desp = query('SELECT DISTINCT "Número Ano" AS ano FROM despesa ORDER BY ano')
    rec_total = query('SELECT SUM("Valor Arrecadação Receita") AS total, SUM("Valor Projeto Receita") AS orcado FROM receita')
    desp_total= query('SELECT SUM("Valor Mês Empenhado") AS empenhado, SUM("Valor Mês Liquidado") AS liquidado, SUM("Valor Mês Pago") AS pago FROM despesa')
    rec_count = query('SELECT COUNT(*) AS cnt FROM receita')
    desp_count= query('SELECT COUNT(*) AS cnt FROM despesa')
    rec_2024  = query('SELECT SUM("Valor Arrecadação Receita") AS total FROM receita WHERE "Número Ano"=2024')
    desp_2024 = query('SELECT SUM("Valor Mês Empenhado") AS empenhado FROM despesa WHERE "Número Ano"=2024')
    # Dados mensais por ano
    mensal = {}
    for ano in [r["ano"] for r in anos_rec]:
        rows = query(f'SELECT "Número Mês" AS mes, "Descrição Mês" AS nome, SUM("Valor Arrecadação Receita") AS total FROM receita WHERE "Número Ano"={ano} GROUP BY mes, nome ORDER BY mes')
        mensal[str(ano)] = [{"mes": r["mes"], "nome": r["nome"], "total": r["total"]} for r in rows]
    # Despesa mensal (empenhado) por ano
    desp_mensal = {}
    for ano in [r["ano"] for r in anos_desp]:
        rows = query(f'SELECT "Número Mês" AS mes, "Descrição Mês" AS nome, SUM("Valor Mês Empenhado") AS empenhado FROM despesa WHERE "Número Ano"={ano} GROUP BY mes, nome ORDER BY mes')
        desp_mensal[str(ano)] = [{"mes": r["mes"], "nome": r["nome"], "empenhado": r["empenhado"]} for r in rows]
    # Receita por categoria 2024
    cat_2024 = query('SELECT "Descrição Categoria Econômica Receita" AS categoria, SUM("Valor Arrecadação Receita") AS total FROM receita WHERE "Número Ano"=2024 GROUP BY categoria ORDER BY total DESC')
    # Despesa por secretaria 2024
    sec_2024 = query('SELECT "Descrição Unidade Orçamentária" AS secretaria, SUM("Valor Mês Empenhado") AS empenhado FROM despesa WHERE "Número Ano"=2024 GROUP BY secretaria ORDER BY empenhado DESC NULLS LAST LIMIT 10')
    # Schemas das tabelas principais
    try:
        rec_cols  = [r["name"] for r in query("PRAGMA table_info(receita)")]
        desp_cols = [r["name"] for r in query("PRAGMA table_info(despesa)")]
    except Exception:
        rec_cols, desp_cols = [], []

    # Bases de dados adicionais — com amostra + estatísticas
    try:
        bases = query("SELECT name, label, rows, cols FROM _bases_catalog ORDER BY uploaded_at DESC")
        bases_info = []
        for b in bases:
            cols = json.loads(b["cols"] or "[]")
            col_names = [c["col"] for c in cols]
            detail = {"name": b["name"], "label": b["label"],
                      "registros": b["rows"], "colunas": col_names}
            try:
                # Amostra de 15 linhas
                sample = query(f'SELECT * FROM "base_{b["name"]}" LIMIT 15')
                detail["amostra"] = sample
                # Estatísticas de colunas numéricas
                num_cols = [c["col"] for c in cols if any(t in c["type"].lower()
                            for t in ["int","float","num","real","double","decimal"])][:6]
                if num_cols:
                    parts = [f'SUM("{c}") as "s__{c}", AVG("{c}") as "a__{c}", MIN("{c}") as "mi__{c}", MAX("{c}") as "ma__{c}"'
                             for c in num_cols]
                    stats = query(f'SELECT {", ".join(parts)} FROM "base_{b["name"]}"')
                    detail["stats"] = stats[0] if stats else {}
            except Exception as ex:
                detail["erro"] = str(ex)
            bases_info.append(detail)
    except Exception:
        bases_info = []
    return jsonify({
        "anos_receita": [r["ano"] for r in anos_rec],
        "anos_despesa": [r["ano"] for r in anos_desp],
        "receita_total_arrecadado": rec_total[0]["total"],
        "receita_total_orcado": rec_total[0]["orcado"],
        "despesa_total_empenhado": desp_total[0]["empenhado"],
        "despesa_total_liquidado": desp_total[0]["liquidado"],
        "despesa_total_pago": desp_total[0]["pago"],
        "receita_registros": rec_count[0]["cnt"],
        "despesa_registros": desp_count[0]["cnt"],
        "receita_2024": rec_2024[0]["total"],
        "despesa_2024": desp_2024[0]["empenhado"],
        "receita_mensal": mensal,
        "despesa_mensal": desp_mensal,
        "receita_categoria_2024": [{"categoria": r["categoria"], "total": r["total"]} for r in cat_2024],
        "despesa_secretaria_2024": [{"secretaria": r["secretaria"], "empenhado": r["empenhado"]} for r in sec_2024],
        "bases_adicionais": bases_info,
        "schema_receita": rec_cols,
        "schema_despesa": desp_cols
    })

@app.route("/")
@login_required
def index():
    return render_template("index.html")

@app.route("/receita")
@login_required
def receita_page():
    return render_template("receita.html")

@app.route("/despesa")
@login_required
def despesa_page():
    return render_template("despesa.html")

@app.route("/chat")
@login_required
def chat_page():
    return render_template("chat.html")

@app.route("/bases")
@login_required
def bases_page():
    return render_template("bases.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
