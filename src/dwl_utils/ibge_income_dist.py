from pathlib import Path
import pandas as pd

from sqlalchemy import Engine, MetaData

from dwl_utils import download_sheet_from_url
from sql_utils import upload_dataframe_to_postgres, create_table_from_dataframe

def read_dwl_file(table_path: Path)-> pd.DataFrame:
    table = pd.read_excel(
        table_path,
        header=None,
        skiprows=5,
        usecols="A:E",
        names=[
            "Brasil, Unidade da Federação e Município",
            "Classes de rendimento nominal mensal de todos os trabalhos",
            "Total",
            "Homens",
            "Mulheres",
        ],
    )

    return table

def table_ajust(table: pd.DataFrame, arbitrary_id_suffix: int)-> pd.DataFrame:

    territory_column = "Brasil, Unidade da Federação e Município"
    table = table[
        ~table[territory_column].astype(str).str.startswith("Fonte:", na=False)
    ].copy()
    table[territory_column] = table[territory_column].ffill()

    table["id_tabela"] = (table.index.astype(str) + str(arbitrary_id_suffix)).astype(int)
    table = table[["id_tabela", *[column for column in table.columns if column != "id_tabela"]]]
    
    return table

def loop_dowload(url_dict:dict[str,str],
    download_dir: str | Path,
    schema_name:str,
    table_name:str ,
    engine: Engine,
    chunck_size: int = 50_000,
) -> None:
    
    downloaded_paths: dict[str, Path] = {}
    pending_downloads = url_dict.copy()

    while pending_downloads:
        failed_downloads: dict[str, str] = {}

        for range, url in pending_downloads.items():
            
            try:
                table_path = download_sheet_from_url(url, range, download_dir, table_name, "xlsx")
                downloaded_paths[range] = table_path
            
            except Exception as e:
                print(f"error during {range} data download: {e}")
                failed_downloads[range] = url
                continue

        if failed_downloads:
            print(f"trying again for {list(failed_downloads)}")

        pending_downloads = failed_downloads

    dfs = [
        read_dwl_file(downloaded_paths[range])
        for range in url_dict
    ]

    final_df = [table_ajust(df, i) for i, df in enumerate(dfs)]

    create_table_from_dataframe(final_df[0], engine, MetaData(schema=schema_name), schema_name, table_name)
    
    for df in final_df:
        upload_dataframe_to_postgres(df, engine, schema_name, table_name, chunck_size)