from pathlib import Path
import pandas as pd

from sqlalchemy import Engine, MetaData

from dwl_utils import download_sheet_from_url
from sql_utils import upload_dataframe_to_postgres, create_table_from_dataframe

# função feita pelo codex
def read_age_range_ibge_table(table_path: Path, age_range: str) -> pd.DataFrame:
    table = pd.read_excel(
        table_path,
        header=None,
        skiprows=5,
        usecols="A:F",
        names=[
            "cod_territorio",
            "territorio",
            "forma_declaracao_idade",
            "populacao_total",
            "homens",
            "mulheres",
        ],
    )

    table = table.dropna(how="all")
    table = table[table["cod_territorio"].notna()]
    table = table[~table["cod_territorio"].astype(str).str.startswith("Fonte:")]
    table = table.assign(faixa_etaria=age_range)

    return table

# função feita pelo codex
def table_ajust(dfs: list[pd.DataFrame]):
    concat_df = pd.concat(dfs, ignore_index=True)

    population_by_age_group = concat_df.melt(
        id_vars=["cod_territorio", "territorio", "faixa_etaria"],
        value_vars=["populacao_total", "homens", "mulheres"],
        var_name="populacao",
        value_name="quantidade",
    )

    population_by_age_group["populacao"] = population_by_age_group["populacao"].replace(
        {"populacao_total": "total"}
    )

    population_by_age_group = population_by_age_group.pivot_table(
        index=["cod_territorio", "territorio", "populacao"],
        columns="faixa_etaria",
        values="quantidade",
        aggfunc="first",
    ).reset_index()

    population_by_age_group.columns.name = None
    pop_code = {"homens": 1, "mulheres": 2, "total": 3}
    population_by_age_group["id_tabela"] = (
        population_by_age_group["cod_territorio"].astype(str)
        + population_by_age_group["populacao"].map(pop_code).astype(str)
    ).astype(int)

    population_by_age_group = population_by_age_group[
        ["id_tabela"]
        + [
            column
            for column in population_by_age_group.columns
            if column not in ["id_tabela", "100_mais"]
        ]
        + ["100_mais"]
    ]

    return population_by_age_group

def download_full_db(
    url_dict:dict[str,str], 
    download_dir: str | Path,
    schema_name:str,
    table_name:str ,
    engine: Engine,
    chunck_size: int = 50_000,
):

    dfs = []

    for range, url in url_dict.items():
        table_path = download_sheet_from_url(url, range, download_dir, table_name, "xlsx")
        dfs.append(read_age_range_ibge_table(table_path, range))

    final_df = table_ajust(dfs)

    create_table_from_dataframe(final_df, engine, MetaData(schema=schema_name), schema_name, table_name)
    upload_dataframe_to_postgres(final_df, engine, schema_name, table_name, chunck_size)
    