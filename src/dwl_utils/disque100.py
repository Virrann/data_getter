from pathlib import Path
from collections.abc import Iterator
from uuid import uuid4

import pandas as pd
from sqlalchemy import Engine, MetaData, inspect

try:
    from .dwl import download_sheet_from_url, safe_read_csv, latin1_to_utf8
except ImportError:
    from dwl import download_sheet_from_url, safe_read_csv, latin1_to_utf8  # type: ignore

from sql_utils import create_table_from_dataframe, upload_dataframe_to_postgres, normalize_column_name


def table_ajust(db: pd.DataFrame) -> pd.DataFrame:
    db.columns = [
        normalize_column_name(latin1_to_utf8(column))
        for column in db.columns
    ]

    rename_map = {
        "data_da_denuncia": "data_de_cadastro",
        "data_da_d": "data_de_cadastro",
        "idade_vitima": "faixa_etaria_da_vitima",
        "idade_suspeito": "faixa_etaria_do_suspeito",
        "genero_da": "genero_da_vitima",
        "deficiencia_vitima": "deficiencia_da_vitima",
        "deficiencia_suspeito": "deficiencia_do_suspeito",
        "grau_instrucao_da_vitima": "grau_de_instrucao_da_vitima",
        "grau_instrucao_do_suspeito": "grau_de_instrucao_do_suspeito",
        "relacao_suspeito_x_vitima": "relacao_vitima_suspeito",
        "relacao_demandante_vitima": "relacao_vitima_suspeito", 
        "pais_de_origem_da_vitima": "pais_da_vitima",
        "pais_de_origem_do_suspeito": "pais_do_suspeito",
        "violacoes": "violacao",
        "motivacoes": "motivacao",
        "agravantes": "agravante",
        "grupo_vuln": "grupo_vulneravel",
        "cenario_da": "cenario_da_violacao",
        "hash_par_vitima_suspeito": "hash",
        "hash_par_vitima_sus": "hash",
        "municipio": "municipio"
    }
    db.rename(columns=rename_map, inplace=True)

    db = db.loc[:, ~db.columns.duplicated()]

    if "data_de_cadastro" not in db.columns:
        colunas_data = [col for col in db.columns if col.startswith("data_")]
        if colunas_data:
            db.rename(columns={colunas_data[0]: "data_de_cadastro"}, inplace=True)
        else:
            db["data_de_cadastro"] = pd.NaT

    db = db.loc[:, ~db.columns.duplicated()]

    text_columns = db.select_dtypes(include=["object", "string"]).columns
    db[text_columns] = db[text_columns].apply(lambda column: column.map(latin1_to_utf8))
    db[text_columns] = db[text_columns].fillna("Não informado")

    db["data_de_cadastro"] = pd.to_datetime(
        db["data_de_cadastro"], errors="coerce"
    )

    invalid_date_mask = db["data_de_cadastro"].isna()
    if invalid_date_mask.any():
        print("Aviso: Existem datas inválidas ou nulas. O id_tabela usará UUID como fallback.")

    if "sl_quantidade_vitimas" in db.columns:
        db["sl_quantidade_vitimas"] = pd.to_numeric(
            db["sl_quantidade_vitimas"], errors="coerce"
        ).astype("Int64")

    db["id_tabela"] = (
        db["data_de_cadastro"].dt.strftime("%Y%m%d%H%M%S").astype("string")
        + db.index.astype("string")
    )
    db.loc[invalid_date_mask, "id_tabela"] = [
        str(uuid4()) for _ in range(int(invalid_date_mask.sum()))
    ]

    ordered_columns = ["id_tabela"] + [col for col in db.columns if col != "id_tabela"]
    db = db[ordered_columns]

    return db


def iter_csv_chunks(file_path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
    try:
        yield from pd.read_csv(
            file_path,
            sep=";",
            encoding="utf-8",
            chunksize=chunk_size,
            low_memory=False
        )
    except UnicodeDecodeError:
        yield from pd.read_csv(
            file_path,
            sep=";",
            encoding="latin1",
            chunksize=chunk_size,
            low_memory=False
        )


def align_to_table_columns(
    db: pd.DataFrame,
    db_columns: list[str],
) -> pd.DataFrame:
    if not db_columns:
        return db

    expected_columns = [col if col != "id" else "id_tabela" for col in db_columns]

    for col in expected_columns:
        if col not in db.columns:
            if col == "sl_quantidade_vitimas":
                db[col] = 0
            else:
                db[col] = "Não informado"

    if "id_tabela" in expected_columns:
        db = db[expected_columns]

    return db


def download_full_db(
    url: str,
    periods: list[str],
    download_dir: str | Path,
    schema_name: str,
    table_name: str,
    engine: Engine,
    chunck_size: int = 50_000,
) -> list[str]:
    periods_with_download_error: list[str] = []

    for period in periods:
        try:
            print(f"Start dowload: {period} data")
            path = download_sheet_from_url(url, period, download_dir, table_name, "csv")
            
            inspector = inspect(engine)
            db_columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema_name)]
            rows_inserted = 0

            for db in iter_csv_chunks(path, chunck_size):
                db = table_ajust(db)
                db = align_to_table_columns(db, db_columns)
                rows_inserted += upload_dataframe_to_postgres(
                    db,
                    engine,
                    schema_name,
                    table_name,
                    chunck_size,
                )

            print(f"Succes on dowload: {period} data")
            print(f"Lines inserted in {schema_name}.{table_name}: {rows_inserted}, from {period} data")

        except Exception as e:
            print(f"error during {period} data dowload: {e}")
            periods_with_download_error.append(period)
            continue

    return periods_with_download_error


def loop_download(
    url: str,
    periods: list[str],
    download_dir: str | Path,
    schema_name: str,
    table_name: str,
    engine: Engine,
    chunck_size: int = 50_000,
) -> None:
    metadata = MetaData(schema=schema_name)

    try:
        path = download_sheet_from_url(url, periods[0], download_dir, table_name, "csv")
        
        first_chunk = next(iter_csv_chunks(path, chunck_size))
        first_chunk = table_ajust(first_chunk)
        create_table_from_dataframe(first_chunk, engine, metadata, schema_name, table_name)
    
    except:
        raise

    periods_with_download_error = download_full_db(
        url,
        periods,
        download_dir,
        schema_name,
        table_name,
        engine,
        chunck_size,
    )

    while periods_with_download_error:
        print(f"trying again for {periods_with_download_error}")
        periods_with_download_error = download_full_db(
            url,
            periods_with_download_error,
            download_dir,
            schema_name,
            table_name,
            engine,
            chunck_size,
        )