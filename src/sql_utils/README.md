este arquivo assim como as docstrings do pacote foram escrita pelo codex
# sql_utils

Utilitários para criar tabelas e carregar DataFrames pandas no PostgreSQL usando SQLAlchemy.

## Funções disponíveis

```python
from sql_utils import (
    build_postgres_engine,
    create_table_from_dataframe,
    drop_table,
    pandas_dtype_to_sqlalchemy,
    print_table_head,
    upload_dataframe_to_postgres,
)
```

## Fluxo básico

1. Criar uma engine PostgreSQL.
2. Preparar um DataFrame com a coluna `id_tabela`.
3. Criar/verificar a tabela de destino.
4. Inserir os dados em chunks com barra de progresso.

```python
from sqlalchemy import MetaData
from sql_utils import (
    build_postgres_engine,
    create_table_from_dataframe,
    upload_dataframe_to_postgres,
)

engine = build_postgres_engine(
    host="localhost",
    port=5432,
    database="meu_banco",
    user="meu_usuario",
    password="minha_senha",
)

metadata = MetaData(schema="raw")

create_table_from_dataframe(
    dataframe=db,
    engine=engine,
    metadata=metadata,
    schema_name="raw",
    table_name="SENASP",
)

rows_inserted = upload_dataframe_to_postgres(
    dataframe=db,
    engine=engine,
    schema_name="raw",
    table_name="SENASP",
    chunk_size=50_000,
)
```

## Requisitos do DataFrame

O DataFrame precisa ter uma coluna chamada `id_tabela`.

Durante a carga:

- `id_tabela` vira a coluna primária `id` no banco;
- registros com o mesmo `id` do chunk atual são deletados antes da reinserção;
- os dados são enviados via `COPY FROM STDIN`, mais rápido que `DataFrame.to_sql`;
- `tqdm` mostra o progresso por chunk.

## Funções

### `build_postgres_engine`

Cria uma engine SQLAlchemy para PostgreSQL usando o driver `psycopg2`.

### `pandas_dtype_to_sqlalchemy`

Converte dtypes do pandas para tipos SQLAlchemy básicos usados na criação automática de tabelas.

### `create_table_from_dataframe`

Cria o schema caso necessário e cria/verifica a tabela com base nas colunas do DataFrame.

### `upload_dataframe_to_postgres`

Insere o DataFrame no PostgreSQL em chunks usando `COPY`. É idempotente para os `id`s enviados, pois remove os registros equivalentes antes de reinserir.

### `print_table_head`

Imprime as primeiras linhas de uma tabela em um schema e retorna o DataFrame consultado.

### `drop_table`

Remove uma tabela caso ela exista.
