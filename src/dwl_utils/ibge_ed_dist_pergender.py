from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, MetaData

try:
    from .dwl import download_sheet_from_url
except ImportError:
    from dwl import download_sheet_from_url  # type: ignore

from sql_utils import create_table_from_dataframe, upload_dataframe_to_postgres


REGION_COLUMN = "Brasil, Grande Região, Unidade da Federação e Município"
EDUCATION_LEVEL_COLUMNS = [
    "Sem instrução e fundamental incompleto",
    "Fundamental completo e médio incompleto",
    "Médio completo e superior incompleto",
    "Superior completo",
]


def read_table(table_path: Path) -> pd.DataFrame:
    raw_table = pd.read_excel(table_path, header=None)
    raw_table = raw_table[~raw_table[0].astype(str).str.startswith("Fonte:", na=False)]

    return raw_table


def table_ajust(raw_table: pd.DataFrame) -> pd.DataFrame:
    population_labels = raw_table.iloc[1].ffill()
    education_level_row = raw_table.iloc[2]
    data_table = raw_table.iloc[3:].copy()
    data_table = data_table[data_table[0].notna()]

    population_tables = []
    for population_label in population_labels.iloc[1:].dropna().unique():
        population_columns = [
            column_index
            for column_index in raw_table.columns[1:]
            if population_labels[column_index] == population_label
        ]

        population_table = data_table[[0, *population_columns]].copy()
        population_table.columns = [
            REGION_COLUMN,
            *education_level_row[population_columns].tolist(),
        ]
        population_table = population_table[
            [REGION_COLUMN, *EDUCATION_LEVEL_COLUMNS]
        ]
        population_table.insert(1, "populacao", population_label.lower())
        population_tables.append(population_table)

    final_df = pd.concat(population_tables, ignore_index=True)
    final_df[REGION_COLUMN] = (
        final_df[REGION_COLUMN]
        .astype(str)
        .str.replace(r"\s*\([^)]*\)", "", regex=True)
        .str.strip()
    )

    for column in EDUCATION_LEVEL_COLUMNS:
        final_df[column] = pd.to_numeric(final_df[column], errors="coerce").astype(float)

    final_df = final_df.reset_index().rename(columns={"index": "id_tabela"})

    return final_df


def download_full_db(
    url: str,
    download_dir: str | Path,
    schema_name: str,
    table_name: str,
    engine: Engine,
    chunck_size: int = 50_000,
) -> None:
    print(f"Start dowload: {table_name} data")
    path = download_sheet_from_url(url, "", download_dir, table_name, "xlsx")
    df = read_table(path)
    df = table_ajust(df)
    print(f"Succes on dowload: {table_name} data")

    rows_inserted = upload_dataframe_to_postgres(
        df,
        engine,
        schema_name,
        table_name,
        chunck_size,
    )
    print(f"Lines inserted in {schema_name}.{table_name}: {rows_inserted}")


def loop_download(
    url: str,
    download_dir: str | Path,
    schema_name: str,
    table_name: str,
    engine: Engine,
    chunck_size: int = 50_000,
) -> None:
    path = download_sheet_from_url(url, "", download_dir, table_name, "xlsx")
    df = read_table(path)
    df = table_ajust(df)
    create_table_from_dataframe(df, engine, MetaData(schema=schema_name), schema_name, table_name)

    download_full_db(
        url,
        download_dir,
        schema_name,
        table_name,
        engine,
        chunck_size,
    )
