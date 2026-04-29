import os
from pathlib import Path

import pandas as pd

from sqlalchemy import Engine, MetaData

try:
    from .dwl import download_sheet_from_url
except ImportError:
    from dwl import download_sheet_from_url #type: ignore

from sql_utils import upload_dataframe_to_postgres, build_postgres_engine, create_table_from_dataframe

def table_ajust(db: pd.DataFrame):
    """
    Padroniza a base SENASP antes de inserir no PostgreSQL.

    A função cria a coluna ``id_tabela`` a partir do ano de ``data_referencia``
    concatenado com o número sequencial da linha. Também move ``id_tabela`` para
    a primeira posição e converte colunas numéricas da base para inteiros,
    tratando valores ausentes como zero.

    Args:
        db: DataFrame lido da planilha SENASP.

    Returns:
        DataFrame ajustado e pronto para criação/carga da tabela.

    Raises:
        ValueError: Se ``data_referencia`` não puder ser convertida para data.
    """

    year_series = pd.to_datetime(db["data_referencia"], errors="coerce").dt.year

    if year_series.isna().any():
        raise ValueError("The 'data_referencia' column must contain valid dates to generate the id_tabela.")
    
    row_id = pd.Series(range(1, len(db) + 1), index=db.index, dtype="int64")
    db["id_tabela"] = year_series.astype("Int64").astype("string") + row_id.astype("string")
    ordered_columns = ["id_tabela", *[column for column in db.columns if column != "id_tabela"]]
    db = db[ordered_columns]

    db["feminino"] = (
        pd.to_numeric(db["feminino"], errors="coerce")  # garante numérico
        .fillna(0)                                     # NaN → 0
        .astype(int)
    )
    db["masculino"] = (
        pd.to_numeric(db["masculino"], errors="coerce")  # garante numérico
        .fillna(0)                                     # NaN → 0
        .astype(int)
    )
    db["nao_informado"] = (
        pd.to_numeric(db["nao_informado"], errors="coerce")  # garante numérico
        .fillna(0)                                     # NaN → 0
        .astype(int)
    )
    db["total_vitima"] = (
        pd.to_numeric(db["total_vitima"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    return db



def download_full_db(
    url:str,
    years:list[int],
    download_dir: str | Path,
    schema_name:str,
    table_name:str,
    engine: Engine,
    chunck_size: int = 50_000,
) -> list[int]:
    """
    Baixa, ajusta e carrega bases SENASP no PostgreSQL.

    A função percorre os anos informados, baixa cada arquivo, aplica
    ``table_ajust`` e insere os dados no PostgreSQL usando
    ``upload_dataframe_to_postgres``.

    Args:
        url: URL com placeholder ``{y}`` para o ano da base.
        years: Lista de anos a baixar e carregar.
        download_dir: Diretório base para salvar os arquivos baixados.
        schema_name: Schema PostgreSQL de destino.
        table_name: Nome da tabela e prefixo dos arquivos baixados.
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
        chunck_size: Quantidade de linhas por chunk na carga para o banco.

    Returns:
        Lista de anos que falharam durante download, leitura ou ajuste.
    """

    years_with_download_error: list[int] = []

    for y in years:
        try:
            print(f"Start dowload: {y} data")
            path = download_sheet_from_url(url, y, download_dir, table_name, "xlsx")
            db = pd.read_excel(path)
            db = table_ajust(db)
            print(f"Succes on dowload: {y} data")

        except Exception as e:
            print(f"error during {y} data dowload: {e}")
            years_with_download_error.append(y)
            continue
        try:
            rows_inserted = upload_dataframe_to_postgres(
                db,
                engine,
                schema_name,
                table_name,
                chunck_size
            )

            print(f"Lines inserted in {schema_name}.{table_name}: {rows_inserted}, from {y} data")

        except:
            raise

    return years_with_download_error


def loop_download(
    url: str,
    years: list[int],
    download_dir: str | Path,
    schema_name: str,
    table_name: str,
    chunck_size: int = 50_000
) -> None:
    """
    Repete a carga SENASP até todos os anos baixarem com sucesso.

    A função chama ``download_full_db`` e, enquanto ela retornar uma lista não
    vazia, executa novamente apenas para os anos que falharam no download,
    leitura ou ajuste.

    Args:
        url: URL com placeholder ``{y}`` para o ano da base.
        years: Lista inicial de anos a baixar e carregar.
        download_dir: Diretório base para salvar os arquivos baixados.
        schema_name: Schema PostgreSQL de destino.
        table_name: Nome da tabela e prefixo dos arquivos baixados.
    """

    engine = build_postgres_engine(
        "localhost",
        int(os.environ.get("POSTGRES_PORT", 5432)),
        os.environ["POSTGRES_DB"],
        os.environ["POSTGRES_USER"],
        os.environ["POSTGRES_PASSWORD"]
    )
    metadata = MetaData(schema=schema_name)

    try:
        path = download_sheet_from_url(url, years[0], download_dir, table_name, "xlsx")
        db = pd.read_excel(path)
        db = table_ajust(db)
        create_table_from_dataframe(db, engine, metadata, schema_name, table_name)

    except:
        raise

    years_with_download_error = download_full_db(
        url,
        years,
        download_dir,
        schema_name,
        table_name,
        engine,
        chunck_size
    )

    while years_with_download_error:
        print(f"trying again for {years_with_download_error}")
        years_with_download_error = download_full_db(
            url,
            years_with_download_error,
            download_dir,
            schema_name,
            table_name,
            engine,
            chunck_size
        )