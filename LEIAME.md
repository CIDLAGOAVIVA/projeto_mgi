# Projeto de raspagem dos das empresas CEITEC, EMBEL E TELEBRAS

Arvore do projeto:

├── prompts
│   ├── CEITEC
│   │   └── prompt_ceitec.md
│   ├── IMBEL
│   │   └── prompt_imbel.md
│   └── TELEBRAS
│       └── prompt_telebras.md
├── pyproject.toml
├── raspagem
│   └── scripts
│       └── main.py
└── README.md

para renderizar uma nova arvore com o comando `tree`:

```bash
tree -I ".git|__pycache__|.venv|*.pyc"
```


para rodar em segundo plano:

```bash
nohup uv run scripts/CEITEC.py > logs_ceitec.log >&1 &
```