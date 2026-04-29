import re
import unicodedata

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