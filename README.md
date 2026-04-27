# Orçamento Municipal — Plataforma Analítica com IA

Dashboard analítico de execução orçamentária municipal com IA integrada (Claude), construído com Flask + SQLite.

## Stack
- **Backend**: Python / Flask
- **Banco de dados**: SQLite (embutido no repositório)
- **IA**: Claude claude-sonnet-4-20250514 via Anthropic API
- **Deploy**: Railway

---

## Configuração local

```bash
# 1. Clone o repositório
git clone https://github.com/SEU_USUARIO/orcamento-municipal.git
cd orcamento-municipal

# 2. Crie o ambiente virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Configure a variável de ambiente
export ANTHROPIC_API_KEY=sk-ant-...

# 5. Rode localmente
python app.py
# Acesse: http://localhost:5000
```

---

## Deploy no Railway

### 1. Crie o projeto no Railway
```
https://railway.app → New Project → Deploy from GitHub repo
```

### 2. Conecte o repositório GitHub
- Faça o push do código para o GitHub (veja abaixo)
- No Railway, selecione o repositório

### 3. Configure a variável de ambiente
No painel do Railway → **Variables** → adicione:
```
ANTHROPIC_API_KEY = sk-ant-SUA_CHAVE_AQUI
```

### 4. Deploy automático
O Railway detecta o `Procfile` e `requirements.txt` automaticamente.
Cada `git push` na branch `main` dispara um novo deploy.

---

## Push para o GitHub

```bash
# Na primeira vez
git init
git add .
git commit -m "feat: plataforma analítica orçamentária com IA"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/orcamento-municipal.git
git push -u origin main

# Atualizações futuras
git add .
git commit -m "fix: descrição da mudança"
git push
```

---

## Estrutura do projeto

```
orcamento-municipal/
├── app.py              # Backend Flask + endpoints da API
├── requirements.txt    # Dependências Python
├── Procfile            # Comando de start para Railway
├── railway.toml        # Configuração do Railway
├── .gitignore
├── README.md
├── data/
│   └── municipal.db    # Banco SQLite com dados orçamentários
└── templates/
    └── index.html      # Frontend completo (layout Kallas)
```

## Endpoints da API

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/` | Dashboard principal |
| GET | `/api/anos` | Anos disponíveis |
| GET | `/api/kpis?ano=2024` | KPIs do ano |
| GET | `/api/receita-mensal?ano=2024` | Arrecadação por mês |
| GET | `/api/despesa-funcao?ano=2024` | Despesa por função |
| GET | `/api/receita-categoria?ano=2024` | Receita por categoria |
| POST | `/api/chat` | Proxy seguro para a API do Claude |
