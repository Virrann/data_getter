import re
import unicodedata

from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy import Engine, MetaData, Table, cast, func, update
from sqlalchemy.sql.sqltypes import BigInteger, Integer, SmallInteger

def build_postgres_engine(host: str, port: int, database: str, user: str, password: str):
    """
    Cria uma engine SQLAlchemy para conexão com PostgreSQL via psycopg2.

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

def normalize_column_name(text: str) -> str:
    text = str(text)

    # remover acentos
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))

    # lowercase
    text = text.lower()

    # substituir não alfanumérico por _
    text = re.sub(r"[^a-z0-9]+", "_", text)

    # remover _ duplicados
    text = re.sub(r"_+", "_", text)

    # remover _ no início/fim
    text = text.strip("_")

    # evitar começar com número
    if text and text[0].isdigit():
        text = f"col_{text}"

    return text

def normalize_table_columns_by_factor(
    engine: Engine,
    schema_name: str,
    table_name: str,
    columns: list[str],
    factor: int | float,
    filters: dict[str, object] | None = None,
) -> None:
    """
    Multiplica colunas numéricas de uma tabela PostgreSQL por um fator.

    A função executa um ``UPDATE`` diretamente no banco usando SQLAlchemy. Se a
    coluna for inteira, o resultado é truncado com ``trunc`` e convertido de
    volta para o tipo inteiro original. Quando ``filters`` é informado, o ajuste
    é aplicado apenas às linhas que atendem todas as igualdades indicadas.

    Args:
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
        schema_name: Schema onde a tabela está localizada.
        table_name: Nome da tabela a normalizar.
        columns: Colunas que serão multiplicadas pelo fator.
        factor: Fator de normalização aplicado às colunas.
        filters: Filtros opcionais no formato ``{coluna: valor}``, combinados
            com ``AND`` no ``WHERE``.
    """

    metadata = MetaData(schema=schema_name)
    table = Table(table_name, metadata, autoload_with=engine)

    values = {}
    integer_types = (SmallInteger, Integer, BigInteger)

    for column_name in columns:
        column = table.c[column_name]
        normalized_value = column * factor

        if isinstance(column.type, integer_types):
            normalized_value = cast(func.trunc(normalized_value), column.type)

        values[column_name] = normalized_value

    statement = update(table).values(**values)

    if filters:
        for filter_column_name, filter_value in filters.items():
            statement = statement.where(table.c[filter_column_name] == filter_value)

    with engine.begin() as connection:
        connection.execute(statement)
