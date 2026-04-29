import os
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, MetaData

try:
    from .dwl import download_sheet_from_url
except ImportError:
    from dwl import download_sheet_from_url  # type: ignore

from sql_utils import build_postgres_engine, create_table_from_dataframe, upload_dataframe_to_postgres


def latin1_to_utf8(value):
    if not isinstance(value, str):
        return value

    try:
        return value.encode("latin1").decode("utf-8")
    except UnicodeError:
        return value


def table_ajust(db: pd.DataFrame) -> pd.DataFrame:
    db = db.copy()

    db.columns = [
        latin1_to_utf8(column).replace("\ufeff", "").strip().lower().replace(" ", "_")
        for column in db.columns
    ]

    text_columns = db.select_dtypes(include=["object", "string"]).columns
    db[text_columns] = db[text_columns].apply(lambda column: column.map(latin1_to_utf8))

    db["data_de_cadastro"] = pd.to_datetime(
        db["data_de_cadastro"], format="%d/%m/%Y %H:%M", errors="coerce"
    )

    if db["data_de_cadastro"].isna().any():
        raise ValueError("The 'data_de_cadastro' column must contain valid dates to generate the id_tabela.")

    db["sl_quantidade_vitimas"] = pd.to_numeric(
        db["sl_quantidade_vitimas"], errors="coerce"
    ).astype("Int64")

    db["id_tabela"] = (
        db["data_de_cadastro"].dt.strftime("%Y%m%d%H%M%S").astype("string")
        + db.index.astype("string")
    )

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
            db = pd.read_csv(path, sep=";", encoding="latin1")
            db = table_ajust(db)
            print(f"Succes on dowload: {period} data")

        except Exception as e:
            print(f"error during {period} data dowload: {e}")
            periods_with_download_error.append(period)
            continue

        try:
            rows_inserted = upload_dataframe_to_postgres(
                db,
                engine,
                schema_name,
                table_name,
                chunck_size,
            )

            print(f"Lines inserted in {schema_name}.{table_name}: {rows_inserted}, from {period} data")

        except Exception:
            raise

    return periods_with_download_error


def loop_download(
    url: str,
    periods: list[str],
    download_dir: str | Path,
    schema_name: str,
    table_name: str,
    chunck_size: int = 50_000,
) -> None:
    engine = build_postgres_engine(
        "localhost",
        int(os.environ.get("POSTGRES_PORT", 5432)),
        os.environ["POSTGRES_DB"],
        os.environ["POSTGRES_USER"],
        os.environ["POSTGRES_PASSWORD"],
    )
    metadata = MetaData(schema=schema_name)

    try:
        path = download_sheet_from_url(url, periods[0], download_dir, table_name, "csv")
        db = pd.read_csv(path, sep=";", encoding="latin1")
        db = table_ajust(db)
        create_table_from_dataframe(db, engine, metadata, schema_name, table_name)
    
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
        print(f"tentando novamente para {periods_with_download_error}")
        periods_with_download_error = download_full_db(
            url,
            periods_with_download_error,
            download_dir,
            schema_name,
            table_name,
            engine,
            chunck_size,
        )
