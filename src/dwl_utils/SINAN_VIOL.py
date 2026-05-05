# Precisamos avaliar se a Var cicl_vid é o ciclo da vitima ou agressor

from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
import pandas as pd

from sqlalchemy import Engine, MetaData

from dbfread import DBF
from datasus_dbc import decompress

try:
    from .dwl import download_files_from_ftp
except ImportError:
    from dwl import download_files_from_ftp #type: ignore

from sql_utils import upload_dataframe_to_postgres, create_table_from_dataframe


def get_datasus_dbf_path(path: str | Path) -> Path:
    path = Path(path)
    dbf_path = path.with_suffix(".dbf")

    if not dbf_path.exists():
        decompress(str(path), str(dbf_path))

    return dbf_path


def read_datasus_dbc(path: str | Path, encoding: str = "latin1") -> pd.DataFrame:
    dbf_path = get_datasus_dbf_path(path)
    table = DBF(
        dbf_path,
        encoding=encoding,
        load=True,
    )

    return pd.DataFrame(iter(table))


def iter_datasus_dbc_chunks(
    path: str | Path,
    chunk_size: int,
    encoding: str = "latin1",
) -> Iterator[pd.DataFrame]:
    dbf_path = get_datasus_dbf_path(path)
    table = DBF(
        dbf_path,
        encoding=encoding,
        load=False,
    )

    rows: list[dict] = []
    start_index = 0
    for row in table:
        rows.append(row)
        if len(rows) == chunk_size:
            yield pd.DataFrame(rows, index=range(start_index, start_index + len(rows)))
            start_index += len(rows)
            rows = []

    if rows:
        yield pd.DataFrame(rows, index=range(start_index, start_index + len(rows)))


def convert_column_types(
    dataframe: pd.DataFrame,
    column_types: dict[str, type],
) -> pd.DataFrame:
    dataframe = dataframe.copy()

    for column, target_type in column_types.items():
        if target_type is int:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").astype("Int64")
        elif target_type is float:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce").astype(float)
        elif target_type is str:
            dataframe[column] = dataframe[column].astype("string")
        elif target_type in (datetime, date, pd.Timestamp):
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")
        elif target_type is bool:
            dataframe[column] = dataframe[column].astype("boolean")
        else:
            raise TypeError(f"Unsupported target type for {column}: {target_type}")

    return dataframe


def replace_string_column_values(
    dataframe: pd.DataFrame,
    column: str,
    value_mapping: dict[str, str],
) -> pd.DataFrame:
    if not pd.api.types.is_string_dtype(dataframe[column]):
        raise TypeError(f"Column {column} must be string dtype.")

    dataframe = dataframe.copy()
    dataframe[column] = dataframe[column].replace(value_mapping)

    return dataframe

def table_ajust(df: pd.DataFrame) -> pd.DataFrame:

    df["id_tabela"] = (
        df["NU_ANO"].astype(str)
        + df.index.astype(str)
    )

    df = df[["id_tabela", *[column for column in df.columns if column != "id_tabela"]]]

    column_types = {
        "id_tabela": str,
        "TP_NOT":            str,
        "ID_AGRAVO":         str,
        "DT_NOTIFIC":   datetime,
        "SEM_NOT":           str,
        "NU_ANO":            int,
        "SG_UF_NOT":         str,
        "ID_MUNICIP":        int,
        "ID_UNIDADE":        int,
        "DT_OCOR":       datetime,
        "SEM_PRI":           str,
        "ANO_NASC":          int,
        "NU_IDADE_N":        int,
        "CS_SEXO":           str,
        "CS_GESTANT":        str,
        "CS_RACA":           str,
        "CS_ESCOL_N":        str,#
        "SG_UF":             str,
        "ID_MN_RESI":        int,#
        "ID_PAIS":           str,#
        "NDUPLIC":           str,#
        "DT_INVEST":    datetime,# esse não aparece nas legendas
        "ID_OCUPA_N":        str,#
        "SIT_CONJUG":        str,#
        "SG_UF_OCOR":        str,#
        "ID_MN_OCOR":        int,#
        "HORA_OCOR":         str,#
        "LOCAL_OCOR":        str,#
        "LOCAL_ESPE":        str,#
        "OUT_VEZES":         str,#
        "LES_AUTOP":         str,#
        "VIOL_FISIC":        str,
        "VIOL_PSICO":        str,
        "VIOL_TORT":         str,
        "VIOL_SEXU":         str,
        "VIOL_TRAF":         str,
        "VIOL_FINAN":        str,
        "VIOL_NEGLI":        str,
        "VIOL_INFAN":        str,
        "VIOL_LEGAL":        str,
        "VIOL_OUTR":         str,
        "VIOL_ESPEC":        str,
        "AG_FORCA":          str,
        "AG_ENFOR":          str,
        "AG_OBJETO":         str,
        "AG_CORTE":          str,
        "AG_QUENTE":         str,
        "AG_ENVEN":          str,
        "AG_FOGO":           str,
        "AG_AMEACA":         str,
        "AG_OUTROS":         str,
        "AG_ESPEC":          str,
        "SEX_ASSEDI":        str,
        "SEX_ESTUPR":        str,
        "SEX_PUDOR":         str,
        "SEX_PORNO":         str,
        "SEX_EXPLO":         str,
        "SEX_OUTRO":         str,
        "SEX_ESPEC":         str,
        "PEN_ORAL":          str,
        "PEN_ANAL":          str,
        "PEN_VAGINA":        str,
        "LESAO_NAT":         str,
        "LESAO_ESPE":        str,
        "LESAO_CORP":        str,
        "NUM_ENVOLV":        str,
        "REL_SEXUAL":        str,
        "REL_PAI":           str,
        "REL_MAE":           str,
        "REL_PAD":           str,
        "REL_CONJ":          str,
        "REL_EXCON":         str,
        "REL_NAMO":          str,
        "REL_EXNAM":         str,
        "REL_FILHO":         str,
        "REL_DESCO":         str,
        "REL_IRMAO":         str,
        "REL_CONHEC":        str,
        "REL_CUIDA":         str,
        "REL_PATRAO":        str,
        "REL_INST":          str,
        "REL_POL":           str,
        "REL_PROPRI":        str,
        "REL_OUTROS":        str,
        "REL_ESPEC":         str,
        "AUTOR_SEXO":        str,
        "AUTOR_ALCO":        str,
        "ENC_SAUDE":         str,
        "ENC_TUTELA":        str,
        "ENC_VARA":          str,
        "ENC_ABRIGO":        str,
        "ENC_SENTIN":        str,
        "ENC_DEAM":          str,
        "ENC_DPCA":          str,
        "ENC_DELEG":         str,
        "ENC_MPU":           str,
        "ENC_MULHER":        str,
        "ENC_CREAS":         str,
        "ENC_IML":           str,
        "ENC_OUTR":          str,
        "ENC_ESPEC":         str,
        "REL_TRAB":          str,
        "REL_CAT":           str,
        "CIRC_LESAO":        str,
        "CLASSI_FIN":        str,
        "EVOLUCAO":          str,
        "DT_OBITO":     datetime,
        "DT_DIGITA":    datetime,
        "DT_TRANSUS":   datetime,
        "DT_TRANSDM":   datetime,
        "DT_TRANSSM":   datetime,
        "DT_TRANSRM":   datetime,
        "DT_TRANSRS":   datetime,
        "DT_TRANSSE":   datetime,
        "REL_MAD":           str,
        "TPUNINOT":          str,
        "ORIENT_SEX":        str,
        "IDENT_GEN":         str,
        "VIOL_MOTIV":        str,
        "CICL_VID":          str, # Isso precisamos descobrir se é da vitima ou do autor
        "REDE_SAU":          str,
        "ASSIST_SOC":        str,
        "REDE_EDUCA":        str,
        "ATEND_MULH":        str,
        "CONS_TUTEL":        str,
        "CONS_IDO":          str,
        "DELEG_IDOS":        str,
        "DIR_HUMAN":         str,
        "MPU":               str,
        "DELEG_CRIA":        str,
        "DELEG_MULH":        str,
        "DELEG":             str,
        "INFAN_JUV":         str,
        "DEFEN_PUBL":        str,
        "DT_ENCERRA":   datetime,
    }

    TP_NOT:dict = {
        "1": "Negativa",
        "2": "Individual",
        "3": "Surto",
        "4": "Agregado"
    }

    df = replace_string_column_values (df, "TP_NOT", TP_NOT)

    CS_GESTANT: dict = {
        "1": "1º Trimestre",
        "2": "2º Trimestre",
        "3": "3º Trimestre",
        "4": "Idade gestacional ignorada",
        "5": "Não",
        "6": "Não se aplica",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "CS_GESTANT", CS_GESTANT)

    CS_RACA: dict = {
        "1":"branca",
        "2":"preta",
        "3":"amarela",
        "4":"parda",
        "5":"indígena",
        "9":"Ignorado",
    }

    df = replace_string_column_values (df, "CS_RACA", CS_RACA)

    CS_ESCOL_N:dict = {
        "43": "Analfabeto",
        "1": "1ª a 4ª série incompleta do EF",
        "2": "4ª série completa do EF",
        "3": "5ª à 8ª série incompleta do EF",
        "4": "Ensino fundamental completo ",
        "5": "Ensino médio incompleto",
        "6": "Ensino médio completo",
        "7": "Educação superior incompleta",
        "8": "Educação superior completa",
        "9": "Ignorado",
        "10": "Não se aplica",
    }

    df = replace_string_column_values (df, "CS_ESCOL_N", CS_ESCOL_N)

    NDUPLIC: dict ={
        "0": pd.NA,
        "1": "Não é duplicidade (não listar)",
        "2": "Duplicidade (não contar)"
    }

    df = replace_string_column_values (df, "NDUPLIC", NDUPLIC)

    SIT_CONJUG:dict = {
        "1": "Solteiro",
        "2": "Casado/ União consensual",
        "3": "Viúvo",
        "4": "Separado",
        "8": "Não se aplica",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "SIT_CONJUG", SIT_CONJUG)

    ORIENT_SEX: dict = {
        "1": "Heterossexual",
        "2": "Homossexual",
        "3": "Bissexual",
        "8": "Não se aplica",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "ORIENT_SEX", ORIENT_SEX)

    IDENT_GEN: dict = {
        "1": "Travesti",
        "2": "Transexual Mulher",
        "3": "Transexual Homem",
        "8": "Não se aplica",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "IDENT_GEN", IDENT_GEN)

    LOCAL_OCOR: dict = {
        "01": "Residência",
        "02": "Habitação coletiva",
        "03": "Escola",
        "04": "Local de prática esportiva",
        "05": "Bar ou similar",
        "06": "Via publica",
        "07": "Comércio/Serviços",
        "08": "Industrias/ construção",
        "09": "Outro",
        "99": "Ignorado",
    }

    df = replace_string_column_values (df, "LOCAL_OCOR", LOCAL_OCOR)

    OUT_VEZES: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "OUT_VEZES", OUT_VEZES)

    LES_AUTOP: dict = {
        "1": "Sim",
        "2": "Não",
        "8": "Não se aplica",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "LES_AUTOP", LES_AUTOP)

    VIOL_MOTIV: dict = {
        "01": "Sexismo",
        "02": "LGBTfobia",
        "03": "Racismo",
        "04": "Intolerância religiosa",
        "05": "Xenofobia",
        "06": "Conflito geracional",
        "07": "Situação de rua",
        "08": "Deficiência",
        "09": "Outros",
        "88": "Não se aplica",
        "99": "Ignorado",
    }

    df = replace_string_column_values (df, "VIOL_MOTIV", VIOL_MOTIV)

    VIOL: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    viols = [
        "VIOL_FISIC", "VIOL_PSICO", "VIOL_TORT", "VIOL_TRAF",
        "VIOL_FINAN", "VIOL_NEGLI", "VIOL_INFAN", "VIOL_LEGAL",
        "VIOL_OUTR", "VIOL_SEXU"
    ]

    for v in viols:
        df = replace_string_column_values (df, v, VIOL)

    AG: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    ags:list[str] = [
        "AG_FORCA", "AG_ENFOR", "AG_OBJETO", 
        "AG_CORTE", "AG_QUENTE", "AG_ENVEN",
        "AG_FOGO", "AG_AMEACA", "AG_OUTROS",                 
        ]

    for ag in ags:
        df = replace_string_column_values (df, ag, AG)

    SEX: dict = {
        "1": "Sim",
        "2": "Não",
        "8": "Não se aplica",
        "9": "Ignorado",
    }

    sxs = [
        "SEX_ASSEDI", "SEX_ESTUPR", "SEX_PORNO", 
        "SEX_EXPLO", "SEX_OUTRO"
        ]

    for sx in sxs:
        df = replace_string_column_values (df, sx, SEX)

    NUM_ENVOLV: dict = {
        "1": "Um",
        "2": "Dois ou mais",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "NUM_ENVOLV", NUM_ENVOLV)

    REL: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    rels = [
        "REL_PAI", "REL_MAE", "REL_PAD", "REL_MAD", "REL_CONJ", 'REL_EXCON', 
        'REL_NAMO', 'REL_EXNAM', 'REL_FILHO', 'REL_IRMAO', 'REL_CONHEC', 'REL_DESCO',
        'REL_CUIDA', 'REL_PATRAO', 'REL_INST', 'REL_POL', 'REL_PROPRI', 'REL_OUTROS',
        ]

    for rl in rels:
        df = replace_string_column_values (df, rl, REL)

    AUTOR_SEXO: dict = {
        "1": "Masculino",
        "2": "Feminino",
        "3": "Ambos os sexos",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "AUTOR_SEXO", AUTOR_SEXO)

    AUTOR_ALCO: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "AUTOR_ALCO", AUTOR_ALCO)

    ENC: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    encs = [
        "ENC_SAUDE", "ASSIST_SOC", "REDE_EDUCA", "ATEND_MULH", "CONS_TUTEL",
        "CONS_IDO", "DELEG_IDOS", "DIR_HUMAN", "MPU", "DELEG_CRIA", "DELEG_MULH",
        "DELEG", "INFAN_JUV", "DEFEN_PUBL"
    ]


    for enc in encs:
        df = replace_string_column_values (df, enc, ENC)


    CICL_VID: dict = {
        "1": "Criança",
        "2": "Adolescente",
        "3": "Jovem",
        "4": "Pessoa adulta",
        "5": "Pessoa idosa",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "CICL_VID", CICL_VID)

    REL_TRAB: dict = {
        "1": "Sim",
        "2": "Não",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "REL_TRAB", REL_TRAB)

    REL_CAT: dict = {
        "1": "Sim",
        "2": "Não",
        "8": "Não se aplica",
        "9": "Ignorado",
    }

    df = replace_string_column_values (df, "REL_CAT", REL_CAT)

    allowed = list(column_types.keys())
    df = df.reindex(columns=allowed)

    df = convert_column_types(df, column_types)

    return df

def upload_full_db(
    years: list[str],
    download_dir: Path,
    schema_name: str,
    table_name:str,
    engine: Engine,
    chunck_size: int = 50_000,
)-> None:

    for y in years:
        rows_inserted = 0
        file_path = download_dir / table_name / y
        try:
            print(f"reading: {file_path}")
            for df in iter_datasus_dbc_chunks(file_path, chunck_size):
                df = table_ajust(df)
                rows_inserted += upload_dataframe_to_postgres(
                    df,
                    engine,
                    schema_name,
                    table_name,
                    chunck_size
                )

        except ValueError:
            raise ValueError(f"No such file or directory {file_path}")
        except:
            raise

        print(f"Lines inserted in {schema_name}.{table_name}: {rows_inserted}, from {y} data")

def loop_download(
    url: str,
    years: list[str],
    download_dir: Path,
    schema_name: str,
    table_name: str,
    engine: Engine,
    chunck_size: int = 50_000
) -> None:
        
    metadata = MetaData(schema=schema_name)
    print("start dowload from ftp")
    fail:list[str] = download_files_from_ftp(url, years, download_dir / table_name)#type: ignore
    
    try:
        print("creating table on db")
        df = next(iter_datasus_dbc_chunks(download_dir / table_name / years[-1], chunck_size)) #type: ignore
        df = table_ajust(df)
        create_table_from_dataframe(df, engine, metadata, schema_name, table_name)
    except:
        raise

    upload_full_db(years, download_dir, schema_name, table_name, engine, chunck_size)

    if fail:
        raise Exception(f"faile to dowload {fail}")