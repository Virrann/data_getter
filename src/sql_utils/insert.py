from io import StringIO

import pandas as pd
from tqdm import tqdm
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    inspect,
    Engine,
    text
)
from sqlalchemy.engine import URL
from sqlalchemy.schema import CreateSchema


def build_postgres_engine(host: str, port: int, database: str, user: str, password: str):
    """Cria uma engine SQLAlchemy para conexão com PostgreSQL via psycopg2.

    Args:
        host: Host do servidor PostgreSQL.
        port: Porta do PostgreSQL.
        database: Nome do banco de dados.
        user: Usuário do banco.
        password: Senha do usuário.

    Returns:
        Engine SQLAlchemy pronta para executar comandos e transações.
    """

    return create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )
    )


def pandas_dtype_to_sqlalchemy(dtype):
    """Converte um dtype do pandas para um tipo básico do SQLAlchemy.

    A função cobre os tipos usados na criação automática de tabelas a partir de
    DataFrames. Tipos não reconhecidos são tratados como texto.

    Args:
        dtype: dtype de uma coluna pandas.

    Returns:
        Classe de tipo SQLAlchemy correspondente.
    """

    if pd.api.types.is_integer_dtype(dtype):
        return BigInteger
    if pd.api.types.is_float_dtype(dtype):
        return Float
    if pd.api.types.is_bool_dtype(dtype):
        return Boolean
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    return Text


def create_table_from_dataframe(
    dataframe: pd.DataFrame,
    engine: Engine,
    metadata: MetaData,
    schema_name: str,
    table_name: str,
) -> Table:
    """Cria uma tabela PostgreSQL a partir das colunas de um DataFrame.

    O DataFrame precisa conter a coluna ``id_tabela``. Ela é usada como origem
    lógica do identificador, mas a tabela criada recebe uma coluna primária
    chamada ``id``. As demais colunas são inferidas a partir dos dtypes pandas.
    O schema é criado automaticamente caso ainda não exista.

    Args:
        dataframe: DataFrame usado como referência para nomes e tipos das colunas.
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
        metadata: Objeto MetaData usado para registrar a tabela.
        schema_name: Nome do schema onde a tabela será criada.
        table_name: Nome da tabela a criar ou verificar.

    Returns:
        Objeto SQLAlchemy Table criado.

    Raises:
        KeyError: Se o DataFrame não tiver a coluna ``id_tabela``.
    """

    if "id_tabela" not in dataframe.columns:
        raise KeyError("O dataframe precisa ter a coluna 'id_tabela'.")

    columns = [Column("id", String, primary_key=True)]

    for column_name, dtype in dataframe.dtypes.items():

        column_name = str(column_name)

        if column_name == "id_tabela":
            continue

        columns.append(
            Column(
                column_name,
                pandas_dtype_to_sqlalchemy(dtype),
                nullable=bool(dataframe[column_name].isna().any()),
            )
        )

    table = Table(table_name, metadata, *columns)

    with engine.begin() as connection:
        if not inspect(connection).has_schema(schema_name):
            connection.execute(CreateSchema(schema_name))
        metadata.create_all(connection, tables=[table], checkfirst=True)

    return table

def upload_dataframe_to_postgres(
    dataframe: pd.DataFrame,
    engine: Engine,
    schema_name: str,
    table_name: str,
    chunk_size: int = 50_000,
) -> int:
    """Insere um DataFrame no PostgreSQL em chunks usando COPY.

    A coluna ``id_tabela`` é convertida para a chave primária ``id`` antes da
    carga. Para permitir reexecuções do notebook, cada chunk remove previamente
    da tabela os registros com os mesmos ``id`` e depois insere os dados via
    ``COPY FROM STDIN``. A função exibe uma barra de progresso com tqdm.

    Args:
        dataframe: DataFrame com os dados a inserir. Deve conter ``id_tabela``.
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
        schema_name: Schema de destino.
        table_name: Tabela de destino.
        chunk_size: Quantidade máxima de linhas enviadas por chunk.

    Returns:
        Número de linhas enviadas para o banco.

    Raises:
        KeyError: Se ``id_tabela`` não existir.
        ValueError: Se ``id_tabela`` tiver nulos ou se ``chunk_size`` for inválido.
    """

    if "id_tabela" not in dataframe.columns:
        raise KeyError("O dataframe precisa ter a coluna 'id_tabela'.")
    if dataframe["id_tabela"].isna().any():
        raise ValueError("A coluna 'id_tabela' não pode ter valores nulos.")

    frame = dataframe.copy()
    id_series = frame["id_tabela"].astype("string").str.replace(r"\.0+$", "", regex=True)
    frame.insert(0, "id", id_series)
    frame = frame.drop(columns=["id_tabela"])

    if chunk_size <= 0:
        raise ValueError("chunk_size precisa ser maior que zero.")

    columns = ", ".join(f'"{column}"' for column in frame.columns)
    copy_sql = f"COPY \"{schema_name}\".\"{table_name}\" ({columns}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    delete_sql = f'DELETE FROM "{schema_name}"."{table_name}" WHERE id = ANY(%s)'
    total_rows = len(frame)
    inserted_rows = 0

    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            progress_bar = tqdm(
                range(0, total_rows, chunk_size),
                total=(total_rows + chunk_size - 1) // chunk_size,
                desc=f"Inserindo {schema_name}.{table_name}",
                unit="chunk",
                ncols=120,
                bar_format="{l_bar}{bar:50}{r_bar}",
            )

            for start in progress_bar:
                chunk = frame.iloc[start:start + chunk_size]
                buffer = StringIO()
                chunk.to_csv(buffer, index=False, header=False, na_rep="\\N")
                buffer.seek(0)

                cursor.execute(delete_sql, (chunk["id"].tolist(),))
                cursor.copy_expert(copy_sql, buffer)
                inserted_rows += len(chunk)
                progress_bar.set_postfix(linhas=inserted_rows)

        raw_connection.commit()
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        raw_connection.close()

    return inserted_rows

def drop_table(schema_name: str, table_name: str, engine: Engine) -> None:
    """Remove uma tabela PostgreSQL caso ela exista.

    Args:
        schema_name: Schema onde a tabela está localizada.
        table_name: Nome da tabela a remover.
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
    """

    with engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS \"{schema_name}\".\"{table_name}\"'))