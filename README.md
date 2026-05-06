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

## Backup e restauração do PostgreSQL

O projeto tem dois scripts na raiz para mover o banco PostgreSQL entre computadores:

```bash
./dump_postgres.sh
./load_postgres.sh
```

Os scripts leem o `.env` e detectam automaticamente se o container configurado em `POSTGRES_CONTAINER_NAME` está rodando em `podman` ou `docker`. A detecção tenta `podman` primeiro e depois `docker`.

Variáveis usadas do `.env`:

- `POSTGRES_CONTAINER_NAME`
- `POSTGRES_DB`
- `POSTGRES_USER`

### Gerar dump

Com o container do PostgreSQL rodando, execute na raiz do projeto:

```bash
./dump_postgres.sh
```

O dump será salvo em:

```text
data/db_dumps/<POSTGRES_DB>_YYYYMMDD_HHMMSS.dump
```

Exemplo:

```text
data/db_dumps/cyberchase_20260506_153000.dump
```

### Copiar o dump para outro computador

No outro computador, copie o arquivo `.dump` para dentro deste projeto, de preferência na pasta:

```text
data/db_dumps/
```

Se a pasta ainda não existir, crie-a:

```bash
mkdir -p data/db_dumps
```

Exemplo de destino:

```text
data/db_dumps/cyberchase_20260506_153000.dump
```

### Carregar dump

No computador de destino, suba o PostgreSQL do projeto com Podman ou Docker e execute:

```bash
./load_postgres.sh data/db_dumps/cyberchase_20260506_153000.dump
```

Atenção: o script de load substitui o banco configurado em `POSTGRES_DB`. Ele derruba conexões abertas, remove o banco atual, cria novamente e carrega o conteúdo do dump.