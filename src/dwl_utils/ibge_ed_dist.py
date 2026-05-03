import os
import re
from pathlib import Path

import pandas as pd

from sqlalchemy import Engine, MetaData

try:
    from .dwl import download_sheet_from_url
except ImportError:
    from dwl import download_sheet_from_url #type: ignore

from sql_utils import upload_dataframe_to_postgres, build_postgres_engine, create_table_from_dataframe


def parse_quarter_start_date(quarter_label: str) -> pd.Timestamp:

    quarter_start_month = {
        "1": 1,
        "2": 4,
        "3": 7,
        "4": 10,
    }
    quarter_number, year = re.search(r"(\d)º trimestre (\d{4})", quarter_label).groups() #type: ignore
    return pd.Timestamp(year=int(year), month=quarter_start_month[quarter_number], day=1)


def read_table(table_path: Path) -> pd.DataFrame:

    raw_table = pd.read_excel(table_path, header=None)
    raw_table = raw_table[~raw_table[0].astype(str).str.startswith("Fonte:", na=False)]

    return raw_table

def table_ajust(df:pd.DataFrame):

    instruction_levels = [
        "Total",
        "Sem instrução e menos de 1 ano de estudo",
        "Ensino fundamental incompleto ou equivalente",
        "Ensino fundamental completo ou equivalente",
        "Ensino médio incompleto ou equivalente",
        "Ensino médio completo ou equivalente",
        "Ensino superior incompleto ou equivalente",
        "Ensino superior completo ou equivalente",
        "Não determinado",
    ]
    
    quarter_labels = df.iloc[3].ffill()
    instruction_label_row = df.iloc[4]

    data_table = df.iloc[5:].copy()
    data_table = data_table[data_table[0].notna()]

    quarter_tables = []
    for quarter_label in quarter_labels.iloc[1:].dropna().unique():
        quarter_columns = [
            column_index
            for column_index in df.columns[1:]
            if quarter_labels[column_index] == quarter_label
        ]

        quarter_table = data_table[[0, *quarter_columns]].copy()
        quarter_table.columns = [
            "Brasil, Unidade da Federação e Município",
            *instruction_label_row[quarter_columns].tolist(),
        ]
        quarter_table = quarter_table[
            ["Brasil, Unidade da Federação e Município", *instruction_levels]
        ]
        quarter_table.insert(1, "trimestre", quarter_label)
        quarter_table.insert(2, "data", parse_quarter_start_date(quarter_label))
        quarter_tables.append(quarter_table)

    final_df = pd.concat(quarter_tables, ignore_index=True)

    final_df["id_tabela"] = (
        final_df["data"].dt.year.astype(str)
        + final_df.index.astype(str)
    ).astype(int)
    final_df = final_df[
        ["id_tabela", *[column for column in final_df.columns if column != "id_tabela"]]
    ]

    return final_df

def download_full_db(
    url:str,
    years:list[int],
    download_dir: str | Path,
    schema_name:str,
    table_name:str,
    engine: Engine,
    chunck_size: int = 50_000,
) -> list[int]:
    
    years_with_download_error: list[int] = []

    for y in years:

        try: 
            print(f"Start dowload: {y} data")
            path = download_sheet_from_url(url, y, download_dir, table_name, "xlsx")
            df = read_table(path)
            df = table_ajust(df)
            print(f"Succes on dowload: {y} data")

        except Exception as e:
            print(f"error during {y} data dowload: {e}")
            years_with_download_error.append(y)
            continue

        try:
            rows_inserted = upload_dataframe_to_postgres(
                df,
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
    engine: Engine,
    chunck_size: int = 50_000
) -> None:
    
    try:
        path = download_sheet_from_url(url, years[0], download_dir, table_name, "xlsx")
        df = read_table(path)
        df = table_ajust(df)
        create_table_from_dataframe(df, engine, MetaData(schema=schema_name), schema_name, table_name)

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