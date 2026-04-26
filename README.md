# Setup do Projeto

Este projeto roda com Docker/Podman Compose, subindo:

- Python + JupyterLab
- PostgreSQL

## 1. Pré-requisitos

Instale um dos dois:

```bash
docker compose version
```

ou

```bash 
podman compose version
```

## Primeira execução

Docker:
```bash
docker compose build --no-cache app
docker compose up --abort-on-container-exit 
```

Podman:
```bash
docker compose build --no-cache app
podman compose up --abort-on-container-exit 
```

Isso vai:

1. Buildar a imagem Python
2. Instalar as dependências via Poetry
3. Subir o PostgreSQL
4. Subir o JupyterLab em [localhost:8.8.8.8](http://localhost:8888/lab)

## Excuções seguintes

Docker:
```bash
docker compose up --abort-on-container-exit
```

Podman:
```bash
podman compose up --abort-on-container-exit
```

## Encerrar tudo

Apenas feche a janela do terminal

## Quando usar --build de novo?

Use --build quando mudar:
- `Dockerfile`
- `pyproject.toml`
- depecdências do projeto
- configurações do ambiente pytho

Não precisa rebuildar quando mudar arquivos em:

- src/
- data/
- notebooks