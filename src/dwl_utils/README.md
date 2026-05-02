este arquivo assim como as docstrings do pacote foram escrita pelo codex
# dwl_utils

Utilitários para baixar arquivos de dados e carregar bases específicas no PostgreSQL.

## Imports principais

```python
from dwl_utils import download_sheet_from_url
from dwl_utils import ibge_age_range, senasp
```

## `download_sheet_from_url`

Baixa uma planilha a partir de uma URL parametrizada por ano e salva o arquivo no cache local.

A URL deve conter `{y}` no lugar do ano:

```python
from pathlib import Path
from dwl_utils import download_sheet_from_url

url = "https://exemplo.gov/base-{y}.xlsx"
download_dir = Path("data/cache")

path = download_sheet_from_url(
    url=url,
    year=2025,
    download_dir=download_dir,
    std_name="SENASP",
    ext="xlsx",
)
```

O arquivo é salvo em:

```text
data/cache/SENASP/SENASP-2025.xlsx
```

A função retorna o `Path` do arquivo salvo.

## Módulo `senasp`

O módulo `senasp` contém o fluxo específico para a base SENASP.

```python
from dwl_utils import senasp
```

### `senasp.table_ajust`

Recebe um DataFrame da planilha SENASP e prepara os dados para carga:

- cria `id_tabela` a partir de `data_referencia` e do número da linha;
- coloca `id_tabela` como primeira coluna;
- converte colunas numéricas como `feminino`, `masculino`, `nao_informado` e `total_vitima` para inteiro;
- troca valores ausentes dessas colunas por zero.

### `senasp.download_full_db`

Baixa, ajusta e insere uma lista de anos da base SENASP no PostgreSQL.

```python
from pathlib import Path
from dwl_utils import senasp

years = [2025, 2024, 2023]
url = "https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/download/dnsp-base-de-dados/bancovde-{y}.xlsx/@@download/file"
download_path = Path("data/cache")

failed_years = senasp.download_full_db(
    url=url,
    years=years,
    download_dir=download_path,
    schema_name="raw",
    table_name="SENASP",
    chunck_size=50_000,
)
```

A função retorna uma lista com os anos que falharam no download, leitura ou ajuste.

As credenciais do PostgreSQL são lidas das variáveis de ambiente:

- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

O host usado atualmente é `localhost`.

### `senasp.loop_download`

Executa `download_full_db` e repete automaticamente apenas para os anos que falharem.

```python
senasp.loop_download(
    url=url,
    years=years,
    download_dir=download_path,
    schema_name="raw",
    table_name="SENASP",
)
```

Esse é o fluxo usado em `src/data_get.Ipynb` para lidar com falhas intermitentes de download.

## Módulo `ibge_age_range`

O módulo `ibge_age_range` contém o fluxo para baixar, consolidar e carregar a tabela de distribuição etária do IBGE/SIDRA.

```python
from dwl_utils import ibge_age_range
```

### `ibge_age_range.read_age_range_ibge_table`

Lê uma planilha Excel de uma única faixa etária baixada do SIDRA. A função:

- ignora as linhas de cabeçalho da exportação;
- mantém as colunas de território, forma de declaração de idade, população total, homens e mulheres;
- remove linhas vazias e a linha final de fonte;
- adiciona a coluna `faixa_etaria` com a chave usada no dicionário de URLs.

### `ibge_age_range.table_ajust`

Recebe uma lista de DataFrames, um para cada faixa etária, e monta uma tabela consolidada:

- cria três linhas por território: `homens`, `mulheres` e `total`;
- transforma cada faixa etária em uma coluna;
- cria `id_tabela` a partir de `cod_territorio` e de um código interno de população (`homens = 1`, `mulheres = 2`, `total = 3`);
- mantém `id_tabela` como primeira coluna e `100_mais` como última coluna.

### `ibge_age_range.loop_dowload`

Baixa todas as planilhas de distribuição etária, repete apenas os downloads que falharem e, quando todos os arquivos estiverem disponíveis, cria e carrega a tabela final no PostgreSQL.

```python
from pathlib import Path
from dwl_utils import ibge_age_range

urls = {
    "0_4": "https://exemplo.sidra/0_4.xlsx",
    "5_9": "https://exemplo.sidra/5_9.xlsx",
    "100_mais": "https://exemplo.sidra/100_mais.xlsx",
}

download_path = Path("data/cache")

ibge_age_range.loop_dowload(
    url_dict=urls,
    download_dir=download_path,
    schema_name="raw",
    table_name="age_range_ibge",
    engine=engine,
    chunck_size=50_000,
)
```
