#!/usr/bin/env python3
"""
Gera os arquivos estáticos de dados do dashboard.

Conecta ao Java Agent via Cloudflare Tunnel, roda todas as queries
e salva data/dashboard.json e data/ranking.json.

Uso:
    python generate_data.py

Variáveis de ambiente (opcionais):
    AGENT_URL     — URL do túnel (padrão: veja abaixo)
    AGENT_API_KEY — chave de API do Java Agent
"""
import os, json, requests, time, sys

# ── Configuração ──────────────────────────────────────────────────────────────
AGENT_URL     = os.environ.get("AGENT_URL",     "https://membrane-hdtv-qui-casual.trycloudflare.com")
AGENT_API_KEY = os.environ.get("AGENT_API_KEY", "")
TABLE         = os.environ.get("TABLE_PRESC",   "qqhetl.PBS_AI_ANALYTICS")

# ── Helpers ───────────────────────────────────────────────────────────────────
def _headers():
    h = {"Content-Type": "application/json"}
    if AGENT_API_KEY:
        h["X-API-Key"] = AGENT_API_KEY
    return h

def run_query(sql, limit=500):
    sql = sql.strip()
    print(f"  → {sql[:90].replace(chr(10),' ')}...")
    t0 = time.time()
    try:
        resp = requests.post(
            f"{AGENT_URL}/query",
            json={"sql": sql, "limit": limit},
            headers=_headers(),
            timeout=300,
            verify=True,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"  ✗ HTTP {resp.status_code}: {resp.text[:200]}")
        raise
    except Exception as e:
        print(f"  ✗ Erro: {e}")
        raise

    data = resp.json()
    elapsed = time.time() - t0

    # Formato A: {"columns":[...], "rows":[[...],...]}
    if isinstance(data, dict) and "columns" in data and "rows" in data:
        cols = data["columns"]
        rows = [dict(zip(cols, row)) for row in data["rows"]]
    # Formato B: [[header,...],[val,...],...]
    elif isinstance(data, list) and data and isinstance(data[0], list):
        cols = [str(c) for c in data[0]]
        rows = [dict(zip(cols, row)) for row in data[1:]]
    # Formato C: já é lista de dicts
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    print(f"  ✓ {len(rows)} linhas em {elapsed:.1f}s")
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global AGENT_API_KEY

    # Pede a chave se não estiver no ambiente
    if not AGENT_API_KEY:
        AGENT_API_KEY = input("AGENT_API_KEY (deixe vazio se não usar): ").strip()

    print(f"\n🔗 Túnel: {AGENT_URL}")
    print(f"   Tabela: {TABLE}\n")

    # Health check
    try:
        h = requests.get(f"{AGENT_URL}/health", headers=_headers(), timeout=15, verify=True)
        if h.status_code == 200:
            print(f"✓ Túnel ativo — {h.text[:120]}\n")
        else:
            print(f"✗ Health retornou {h.status_code}: {h.text[:200]}")
            sys.exit(1)
    except Exception as e:
        print(f"✗ Não foi possível conectar ao túnel: {e}")
        sys.exit(1)

    os.makedirs("data", exist_ok=True)
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # ── 1. DASHBOARD ──────────────────────────────────────────────────────────
    print("=" * 60)
    print("📊  DASHBOARD — data/dashboard.json")
    print("=" * 60)

    print("\n[1/4] KPIs globais")
    kpis_r = run_query(f"""
        SELECT
            COALESCE(SUM(RX_COUNT_TOTAL), 0)         AS total_receitas,
            COALESCE(SUM(DISPENSED_QTY_TOTAL), 0)     AS total_medicamentos,
            COUNT(DISTINCT DOCTOR_DISPLAY_CD)          AS qtde_medicos,
            COUNT(DISTINCT MANUFACTURER_DESC)          AS qtde_laboratorios,
            COUNT(DISTINCT BRAND_NAME)                 AS qtde_marcas,
            COUNT(DISTINCT COMBINED_MOLECULE_DESC)     AS qtde_moleculas
        FROM {TABLE}
    """)
    kpis = kpis_r[0] if kpis_r else {}

    print("\n[2/4] Evolução por período")
    evolucao = run_query(f"""
        SELECT PERIOD_CD                  AS periodo,
               SUM(RX_COUNT_TOTAL)       AS receitas,
               SUM(DISPENSED_QTY_TOTAL)  AS medicamentos
        FROM {TABLE}
        GROUP BY PERIOD_CD
        ORDER BY PERIOD_CD
    """)

    print("\n[3/4] Market Share por laboratório (TOP 15)")
    share_raw = run_query(f"""
        SELECT TOP 15
               MANUFACTURER_DESC                    AS nome,
               SUM(RX_COUNT_TOTAL)                  AS receitas,
               SUM(DISPENSED_QTY_TOTAL)              AS medicamentos,
               COUNT(DISTINCT DOCTOR_DISPLAY_CD)     AS medicos
        FROM {TABLE}
        GROUP BY MANUFACTURER_DESC
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
    """, limit=15)
    total_rx = sum(r.get("receitas") or 0 for r in share_raw) or 1
    share = [
        {**r, "share": round((r.get("receitas") or 0) * 100.0 / total_rx, 2)}
        for r in share_raw
    ]

    print("\n[4/4] Distribuição geográfica por estado (TOP 20)")
    geo = run_query(f"""
        SELECT TOP 20
               STATE_DESC                            AS regiao,
               SUM(RX_COUNT_TOTAL)                  AS receitas,
               SUM(DISPENSED_QTY_TOTAL)              AS medicamentos,
               COUNT(DISTINCT DOCTOR_DISPLAY_CD)     AS medicos
        FROM {TABLE}
        GROUP BY STATE_DESC
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
    """, limit=20)

    dashboard = {
        "kpis":         kpis,
        "evolucao":     evolucao,
        "share":        share,
        "geo":          geo,
        "generated_at": now_utc,
    }

    path_dash = os.path.join("data", "dashboard.json")
    with open(path_dash, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False, default=str, indent=2)
    kb = os.path.getsize(path_dash) // 1024
    print(f"\n✅  {path_dash} salvo ({kb} KB)")

    # ── 2. RANKING ────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("👨‍⚕️  RANKING — data/ranking.json")
    print("=" * 60)

    print("\n[1/1] Top 200 prescritores")
    ranking = run_query(f"""
        SELECT TOP 200
               DOCTOR_DISPLAY_CD                             AS crm,
               TRIM(FIRST_NM) || ' ' || TRIM(SURNM_NM)     AS medico,
               CITY_DESC                                     AS cidade,
               STATE_DESC                                    AS estado,
               IMS_BRICK_DESC                                AS brick,
               SUM(RX_COUNT_TOTAL)                          AS total_receitas,
               SUM(DISPENSED_QTY_TOTAL)                     AS total_medicamentos,
               COUNT(DISTINCT MANUFACTURER_DESC)              AS qtde_labs,
               COUNT(DISTINCT BRAND_NAME)                   AS qtde_marcas
        FROM {TABLE}
        GROUP BY DOCTOR_DISPLAY_CD, FIRST_NM, SURNM_NM, CITY_DESC, STATE_DESC, IMS_BRICK_DESC
        ORDER BY SUM(RX_COUNT_TOTAL) DESC
    """, limit=200)

    ranking_data = {
        "ranking":      ranking,
        "generated_at": now_utc,
    }

    path_rank = os.path.join("data", "ranking.json")
    with open(path_rank, "w", encoding="utf-8") as f:
        json.dump(ranking_data, f, ensure_ascii=False, default=str, indent=2)
    kb2 = os.path.getsize(path_rank) // 1024
    print(f"\n✅  {path_rank} salvo ({kb2} KB)")

    # ── Instruções finais ─────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("🎉  Pronto! Faça commit dos arquivos para persistir no deploy:")
    print()
    print("    git add data/dashboard.json data/ranking.json")
    print("    git commit -m 'dados: atualiza snapshot mensal'")
    print("    git push")
    print("=" * 60)


if __name__ == "__main__":
    main()
