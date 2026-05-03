from pathlib import Path
import pandas as pd

from sqlalchemy import Engine, MetaData

from .dwl import download_sheet_from_url
from sql_utils import upload_dataframe_to_postgres, create_table_from_dataframe

def read_dwl_file(table_path: Path, age_range: str) -> pd.DataFrame:
    """
    Lê uma planilha de distribuição etária do IBGE/SIDRA.

    A função ignora as linhas de cabeçalho geradas pela exportação em Excel,
    padroniza as colunas usadas no projeto, remove linhas vazias ou de fonte e
    adiciona a faixa etária correspondente ao arquivo lido.

    Args:
        table_path: Caminho da planilha Excel baixada.
        age_range: Faixa etária representada pela planilha.

    Returns:
        DataFrame com código/nome do território, tipo de declaração de idade,
        população total, homens, mulheres e a faixa etária.
    """

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

def table_ajust(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Consolida planilhas de faixas etárias em uma tabela final larga.

    Cada DataFrame de entrada representa uma faixa etária. A função transforma
    as colunas de população total, homens e mulheres em linhas da coluna
    ``populacao`` e usa as faixas etárias como colunas finais. Também cria
    ``id_tabela`` a partir de ``cod_territorio`` e de um código interno de
    população: homens = 1, mulheres = 2 e total = 3.

    Args:
        dfs: Lista de DataFrames retornados por ``read_dwl_file``.

    Returns:
        DataFrame consolidado com uma linha por território e tipo de população,
        ``id_tabela`` na primeira coluna e ``100_mais`` na última coluna.
    """

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

def loop_dowload(
    url_dict:dict[str,str],
    download_dir: str | Path,
    schema_name:str,
    table_name:str ,
    engine: Engine,
    chunck_size: int = 50_000,
) -> None:
    """
    Baixa, consolida e carrega a distribuição etária do IBGE no PostgreSQL.

    A função tenta baixar todos os pares ``faixa_etaria``/URL informados. Se
    algum download falhar, ela conclui os demais downloads da rodada e depois
    repete apenas os pares que falharam, até que todos os arquivos sejam
    baixados com sucesso. Depois disso, lê as planilhas, monta a tabela final,
    cria a tabela no schema informado e carrega os dados no PostgreSQL.

    Args:
        url_dict: Dicionário em que a chave é a faixa etária e o valor é a URL
            da planilha correspondente.
        download_dir: Diretório base para salvar os arquivos baixados.
        schema_name: Schema PostgreSQL de destino.
        table_name: Nome da tabela e prefixo dos arquivos baixados.
        engine: Engine SQLAlchemy conectada ao PostgreSQL.
        chunck_size: Quantidade de linhas por chunk na carga para o banco.
    """

    downloaded_paths: dict[str, Path] = {}
    pending_downloads = url_dict.copy()

    while pending_downloads:
        failed_downloads: dict[str, str] = {}
        for age_range, url in pending_downloads.items():

            try:
                print(f"Start dowload: {age_range} data")
                table_path = download_sheet_from_url(url, age_range, download_dir, table_name, "xlsx")
                downloaded_paths[age_range] = table_path
                print(f"Succes on dowload: {age_range} data")

            except Exception as error:
                print(f"error during {age_range} data download: {error}")
                failed_downloads[age_range] = url
                continue

        if failed_downloads:
            print(f"trying again for {list(failed_downloads)}")

        pending_downloads = failed_downloads

    dfs = [
        read_dwl_file(downloaded_paths[age_range], age_range)
        for age_range in url_dict
    ]

    final_df = table_ajust(dfs)

    create_table_from_dataframe(final_df, engine, MetaData(schema=schema_name), schema_name, table_name)
    upload_dataframe_to_postgres(final_df, engine, schema_name, table_name, chunck_size)
    