from pathlib import Path
from typing import Protocol

import requests


class SupportsStringFormat(Protocol):
    def __format__(self, format_spec: str, /) -> str:
        ...


def download_sheet_from_url(
        url: str,
        year: SupportsStringFormat,
        download_dir: str | Path,
        std_name: str,
        ext:str | None
    ) -> Path:
    """
    Baixa uma planilha parametrizada por valor formatável e salva no diretório de cache.

    A URL deve conter o placeholder ``{y}``, que é substituído pelo valor informado.
    O arquivo é salvo em ``download_dir/std_name/std_name-year.ext`` e o caminho
    final é retornado para leitura posterior com pandas ou outra biblioteca.

    Args:
        url: URL de download contendo ``{y}`` no ponto onde entra o valor.
        year: Valor formatável usado para montar a URL e o nome do arquivo.
        download_dir: Diretório base onde a pasta ``std_name`` será criada.
        std_name: Nome padrão usado para a subpasta e para o prefixo do arquivo.
        ext: Extensão do arquivo, sem ponto. Quando ``None``, usa ``xlsx``.

    Returns:
        Caminho do arquivo salvo no disco.

    Raises:
        requests.HTTPError: Se a resposta HTTP indicar erro.
        requests.RequestException: Para falhas de rede ou timeout.
    """

    if ext is None:
        ext = "xlsx"

    download_dir = Path(download_dir) / std_name
    download_dir.mkdir(parents=True, exist_ok=True)

    download_url = url.format(y=year)
    file_path = download_dir / f"{std_name}-{year}.{ext}"
    response = requests.get(download_url, timeout=60)
    response.raise_for_status()
    file_path.write_bytes(response.content)

    return file_path