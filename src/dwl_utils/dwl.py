from ftplib import FTP
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

import pandas as pd
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

def download_files_from_ftp(
    directory_url: str,
    file_names: list[str],
    download_dir: str | Path,
    username: str = "anonymous",
    password: str = "anonymous@",
) -> list[Path]:
    """
    Baixa arquivos de um diretório FTP.

    Args:
        directory_url: URL ou caminho FTP até o diretório onde os arquivos estão.
        file_names: Nomes dos arquivos a baixar dentro do diretório FTP.
        download_dir: Diretório local onde os arquivos serão salvos.
        username: Usuário FTP. Por padrão usa login anônimo.
        password: Senha FTP. Por padrão usa senha anônima convencional.

    Returns:
        Lista com os caminhos locais dos arquivos baixados.
    """

    parsed_url = urlparse(directory_url)
    host = parsed_url.hostname or parsed_url.path.split("/", 1)[0]
    remote_dir = parsed_url.path if parsed_url.hostname else ""
    if not parsed_url.hostname and "/" in parsed_url.path:
        remote_dir = parsed_url.path.split("/", 1)[1]

    if not host:
        raise ValueError("directory_url must include an FTP host.")

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    downloaded_files: list[Path] = []
    with FTP(host) as ftp:
        ftp.login(user=username, passwd=password)
        if remote_dir:
            ftp.cwd(remote_dir)

        failed_downloads: dict[str, str] = {}

        for file_name in file_names:
            file_path = download_dir / file_name
            try:
                with file_path.open("wb") as file:
                    ftp.retrbinary(f"RETR {file_name}", file.write)
                downloaded_files.append(file_path)
            except Exception as error:
                failed_downloads[file_name] = str(error)
                if file_path.exists():
                    file_path.unlink()
                continue

    if failed_downloads:
        failed_files = ", ".join(
            f"{file_name} ({error})"
            for file_name, error in failed_downloads.items()
        )
        raise RuntimeError(f"Failed to download FTP files: {failed_files}")

    return downloaded_files


def safe_read_csv(file_path: Path):
    """Tenta ler como UTF-8 primeiro. Se falhar, cai para Latin-1."""
    try:
        return pd.read_csv(file_path, sep=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(file_path, sep=";", encoding="latin1", low_memory=False)
    

def latin1_to_utf8(value: str):
    if not isinstance(value, str):
        return value
    try:
        return value.encode("latin1").decode("utf-8")
    except UnicodeError:
        return value