import pandas as pd
from sqlalchemy import Engine, text

# Gerado pelo codex
def print_table_head(
    engine: Engine,
    schema_name: str,
    table_name: str,
    rows: int = 5,
) -> pd.DataFrame:
    """
    Imprime as primeiras linhas de uma tabela PostgreSQL em um schema.

    A função consulta a tabela informada com ``LIMIT`` e imprime o resultado em
    formato tabular. O DataFrame consultado também é retornado para permitir
    inspeções adicionais quando necessário.

    Args:
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
        schema_name: Schema onde a tabela está localizada.
        table_name: Nome da tabela a consultar.
        rows: Quantidade de linhas a imprimir.

    Returns:
        DataFrame pandas com as linhas consultadas.

    Raises:
        ValueError: Se ``rows`` for menor ou igual a zero.
    """

    if rows <= 0:
        raise ValueError("rows precisa ser maior que zero.")

    preparer = engine.dialect.identifier_preparer
    qualified_table = f"{preparer.quote_schema(schema_name)}.{preparer.quote(table_name)}"
    query = text(f"SELECT * FROM {qualified_table} LIMIT :rows")

    with engine.connect() as connection:
        dataframe = pd.read_sql_query(query, connection, params={"rows": rows})

    print(dataframe.to_string(index=False))
    return dataframe
