from typing import Dict
import urllib.request as url
from .parser.bovesparser import BovesParser
from pathlib import Path
import pandas as pd
import zipfile
import datetime
import os

data_url = "http://bvmf.bmfbovespa.com.br/InstDados/SerHist/"
filename = "COTAHIST_A"

def get_timeline(from_year: int) -> Dict[str, str]:
    data = {}
    to_year = datetime.datetime.now().year
    for year in range(from_year, to_year+1):
        print(f'creating partition year={year}')
        data[f'year={year}'] = ''
    return data

def get_ibov_urls(timeline: Dict[str, str]) -> Dict[str, str]:
    data = timeline
    for year in timeline.keys():
        print(f'writing url ' + year)
        file = filename + year[-4:] + '.ZIP'
        req_url = data_url + file
        data[year] = req_url
    return data

def get_raw_path():
    proj_path = Path.cwd()  # point back to the root of the project
    raw_path = proj_path.joinpath('data/01_raw')
    return str(raw_path.resolve())

def get_ibov_data(ibov_urls: Dict[str, str]) -> Dict[str, str]:
    data = ibov_urls
    path = get_raw_path()
    for key, value in ibov_urls.items():
        print(f'Downloading data from B3: {value}')
        filename = value.split('/')[-1]
        save_file = os.path.join(path, filename)

        with url.urlopen(value) as response, open(save_file, 'wb') as out_file:
            year_data = response.read()
            out_file.write(year_data) 
        df = clean_extract(filename, path)
        data[key] = df
    return data

def clean_extract(file: str, path: str) -> pd.DataFrame:
    with open(os.path.join(path, file), 'rb') as zipdata:
        data = zipfile.ZipFile(zipdata)
        zipinfos = data.infolist()

        # iterate through each file
        for zipinfo in zipinfos:
            # This will do the renaming
            name = zipinfo.filename
            name = name.replace('.', '_')
            name = name[:14] + '.TXT'
            zipinfo.filename = name
            data.extract(zipinfo, path=path)
    strfile = file[:14] + '.TXT'
    df = data_to_csv(strfile, path)
    os.remove(os.path.join(path, strfile))
    os.remove(os.path.join(path, file))
    return df

def data_to_csv(file: str, path: str) -> pd.DataFrame:
    parser = BovesParser(os.path.join(path, file))
    parser.ler_arquivo()
    parser.exportar_csv(os.path.join(path, 'temp.csv'))
    df = pd.read_csv(os.path.join(path, 'temp.csv'), delimiter=';')
    os.remove(os.path.join(path, 'temp.csv'))
    return df