from typing import Dict
import urllib.request as url
from .parser.bovesparser import BovesParser
from pathlib import Path
from kedro.extras.datasets.pandas import JSONDataSet
import pandas as pd
from typing import Callable
import zipfile
from datetime import datetime
from kedro import io
import os

data_url = "http://bvmf.bmfbovespa.com.br/InstDados/SerHist/"
ibov_filename = "COTAHIST_A"

ibov_cols = {"data_pregao", "cod_papel", "nome_resum", "prazo_dias_termo", 
            "moeda","preco_abertura", "preco_maximo", "preco_minimo",
            "preco_medio", "preco_ultimo", "preco_melhor_compra",
            "preco_melhor_venda", "num_negocios", "qtde_titulos", "vol_titulos",
            "preco_exerc", "indicador_correcao", "fator_cotacao", 
            "preco_exerc_pontos"}

def get_todays_date():
    date = datetime.now()
    date_time = date.strftime("%Y-%m-%d")
    return date_time

def get_timeline(from_year: int) -> Dict[str, str]:
    data = {}
    to_year = datetime.now().year
    for year in range(from_year, to_year+1):
        data[f"year={year}"] = ""
    return data

def get_ibov_url(year: str):
    file = ibov_filename + year + ".ZIP"
    req_url = data_url + file
    return req_url

def get_ibov_urls(timeline: Dict[str, str]) -> Dict[str, str]:
    data = timeline
    for year in timeline.keys():
        data[year] = get_ibov_url(year[-4:])
    return data

def get_raw_path():
    proj_path = Path.cwd()  # point back to the root of the project
    raw_path = proj_path.joinpath("data/01_raw")
    return str(raw_path.resolve())

def update_if_outdated(last_updated: Dict[str, str], df: pd.DataFrame):
    today = datetime.now()
    today = datetime(today.year, today.month, today.day)

    last_date = datetime.strptime(last_updated, "%d/%m/%Y")
    if today > last_date:
        year = today.strftime("%Y")
        df[f"year={year}"] = get_ibov_url(year)
    return df

def get_ibov_data(ibov_urls: Dict[str, str], last_updated: str) -> Dict[str, str]:
    data = ibov_urls
    path = get_raw_path()
    ibov_urls = update_if_outdated(last_updated, ibov_urls)

    for key, value in ibov_urls.items():
        print(f"Downloading data from B3: {value}")
        filename = value.split("/")[-1]
        save_file = os.path.join(path, filename)

        with url.urlopen(value) as response, open(save_file, "wb") as out_file:
            year_data = response.read()
            out_file.write(year_data) 
        df = clean_extract(filename, path)
        df = df.drop_duplicates()
        data[key] = df

    return data

def clean_extract(file: str, path: str) -> pd.DataFrame:
    with open(os.path.join(path, file), "rb") as zipdata:
        data = zipfile.ZipFile(zipdata)
        zipinfos = data.infolist()

        # iterate through each file
        for zipinfo in zipinfos:
            # This will do the renaming
            name = zipinfo.filename
            name = name.replace(".", "_")
            name = name[:14] + ".TXT"
            zipinfo.filename = name
            data.extract(zipinfo, path=path)
    strfile = file[:14] + ".TXT"
    df = data_to_csv(strfile, path)
    os.remove(os.path.join(path, strfile))
    os.remove(os.path.join(path, file))
    return df

def data_to_csv(file: str, path: str) -> pd.DataFrame:
    parser = BovesParser(os.path.join(path, file))
    parser.ler_arquivo()
    parser.exportar_csv(os.path.join(path, "temp.csv"))
    df = pd.read_csv(os.path.join(path, "temp.csv"), delimiter=";")
    os.remove(os.path.join(path, "temp.csv"))
    return df

def agg_ibov_csv(ibov_csv: Dict[str, Callable[[], pd.DataFrame]], 
                 last_updated: str) -> pd.DataFrame:
    concattable = []
    for partition, load_csv in ibov_csv.items():
        if int(partition[-4:]) < int(last_updated[-4:]):
            print(f"Partition already extracted: {partition}")
            continue
        else:
            print(f"Updating partition: {partition}")
        csv = load_csv()
        csv = csv.loc[:, ["data_pregao", "cod_papel",
                          "preco_ultimo", "num_negocios"]]
        csv = csv.loc[csv.data_pregao > last_updated, :]
        concattable.append(csv)

    print("Concat started")
    ret = pd.concat(concattable)
    ret = ret.drop_duplicates()
    print(f"Writing: {csv.shape}")
    print(f"Size: {csv.info(memory_usage='deep')}")
    return ret, get_todays_date()
