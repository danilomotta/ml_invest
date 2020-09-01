from typing import Dict, Tuple
import urllib.request as url
from .parser.bovesparser import BovesParser
from pathlib import Path
import pandas as pd
from typing import Callable
import zipfile
from datetime import datetime
import os
import logging

def get_todays_date() -> str: 
    """Get todays date in year-month-day string formar

    Returns:
        str: todays date
    """
    date = datetime.now()
    date_time = date.strftime("%Y-%m-%d")
    return date_time

def get_timeline(from_year: int) -> Dict[str, str]:
    """Returns a dictionary where the keys are years from_year to this year

    Args:
        from_year (int): inicial year of the range

    Returns:
        Dict[str, str]: an empty dictionary containing the years as keys
    """
    data = {}
    to_year = datetime.now().year
    for year in range(from_year, to_year+1):
        data[f"year={year}"] = ""
    return data

def get_ibov_url(year: str, ibov_link: Dict[str, str]) -> str:
    """Given a year returns the url to download the ibov data

    Args:
        year (str): reference year of the dataset to download
        ibov_link (Dict[str, str]): Dict of the dataset url and file name

    Returns:
        str: ibov url to download the file
    """
    file = ibov_link["file"] + year + ".ZIP"
    req_url = ibov_link["url"] + file
    return req_url

def get_ibov_urls(timeline: Dict[str, str], ibov_link: Dict[str, str]) -> Dict[str, str]:
    """Iterate over the years and return a dictionary where the key is the year
    and return a url to download the dataset in the "year" reference

    Args:
        timeline (Dict[str, str]): input dict containing the data years to be downloaded
        ibov_link (Dict[str, str]): Dict of the dataset url and file name

    Returns:
        Dict[str, str]: urls to download the dataset partitions
    """
    data = timeline
    for year in timeline.keys():
        data[year] = get_ibov_url(year[-4:], ibov_link)
    return data

def get_raw_path() -> str:
    """Get the path where the raw data is kept

    Returns:
        str: path
    """
    proj_path = Path.cwd()  # point back to the root of the project
    raw_path = proj_path.joinpath("data/01_raw")
    return str(raw_path.resolve())

def update_if_outdated(last_updated: Dict[str, str], df: pd.DataFrame,
        ibov_link: Dict[str, str]) -> pd.DataFrame:
    """Update the data partitions in df if our data is outdated

    Args:
        last_updated (Dict[str, str]): data of the last update
        df (pd.DataFrame): DataFrame with the dataset partition list
        ibov_link (Dict[str, str]): Dict of the dataset url and file name

    Returns:
        pd.DataFrame: dataframe with the partitions of data
    """
    today = datetime.now()
    today = datetime(today.year, today.month, today.day)

    last_date = datetime.strptime(last_updated, "%Y-%m-%d")
    if today > last_date:
        year = today.strftime("%Y")
        df[f"year={year}"] = get_ibov_url(year, ibov_link)
    return df

def get_ibov_data(ibov_urls: Dict[str, str], last_updated: str,
        ibov_link: Dict[str, str]) -> Dict[str, str]:
    """Download the partitions of the dataset if not already downloaded

    Args:
        ibov_urls (Dict[str, str]): partition x url to be downloaded
        last_updated (str): date of the last update
        ibov_link (Dict[str, str]): Dict of the dataset url and file name

    Returns:
        Dict[str, str]: the downloaded data
    """
    data = ibov_urls
    path = get_raw_path()
    ibov_urls = update_if_outdated(last_updated, ibov_urls, ibov_link)

    log = logging.getLogger(__name__)

    for key, value in ibov_urls.items():
        log.info(f"Downloading data from B3: {value}")
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
    """Extract a zip files containing a IBOV TXT, convert it to DataFranme
    and deletes the temp files

    Args:
        file (str): file to extract
        path (str): path of the file

    Returns:
        pd.DataFrame: DataFrame with the data
    """
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
    """Converts the data in the IBOV TXT to DataFrame using BovesParser

    Args:
        file (str): file name
        path (str): file path

    Returns:
        pd.DataFrame: data extracted
    """
    parser = BovesParser(os.path.join(path, file))
    parser.ler_arquivo()
    parser.exportar_csv(os.path.join(path, "temp.csv"))
    df = pd.read_csv(os.path.join(path, "temp.csv"), delimiter=";")
    os.remove(os.path.join(path, "temp.csv"))
    return df

def agg_ibov_csv(ibov_csv: Dict[str, Callable[[], pd.DataFrame]], 
                 last_updated: str) -> Tuple[pd.DataFrame, str]:
    """Aggragate all data from the partitions collected in a singe CSV

    Args:
        ibov_csv (Dict[str, Callable[[], pd.DataFrame]]): input data loader
        last_updated (str): date of the last update

    Returns:
        pd.DataFrame: aggragated 
    """
    concattable = []
    today = get_todays_date()
    log = logging.getLogger(__name__)
    for partition, load_csv in ibov_csv.items():
        if int(partition[-4:]) < int(last_updated[:4]):
            log.info(f"Partition already extracted: {partition}")
            continue
        elif today > last_updated:
            log.info(f"Getting new data: {partition}")
            csv = load_csv()
            csv = csv.loc[:, ["data_pregao", "cod_papel",
                            "preco_ultimo", "num_negocios"]]
            csv = csv.loc[csv.data_pregao > last_updated, :]
            concattable.append(csv)
        else:
            log.info(f"Partition already extracted today: {partition}")

    if concattable:
        ret = pd.concat(concattable)
        ret = ret.drop_duplicates()
        print(f"New data collected: {ret.shape[0]}")
        return ret, get_todays_date()
    else:
        return pd.DataFrame(), today
    
