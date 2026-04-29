from sqlalchemy.engine import URL
from sqlalchemy import create_engine

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