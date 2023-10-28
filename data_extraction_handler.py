import requests
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from lookups import DataSources, ExtractionErrors, DateCondition
from logging_handler import log_error_msg
from bs4 import BeautifulSoup


def get_data_columns(sources):
    shelter_sources = [source for source in sources if source.name.startswith('SHELTER')]
    data = {}
    for source in shelter_sources:
        response = requests.get(source.value,params={'$limit': 1})
        result = (source.name.lower(), pd.DataFrame(response.json()))
        columns = result[1].columns
        data[result[0]] = columns.tolist()
    return data

def fetch_data(source, limit, etl_date):
    result = None
    condition = None
    try:
        if source.name.startswith("SHELTER"):
            if etl_date != None:
                if source.name.startswith('SHELTER_AUSTIN'):
                    condition = DateCondition.AUSTIN.value.replace('0',f"'{etl_date}'")
                elif source == DataSources.SHELTER_BLOOMINGTON:
                    condition = DateCondition.BLOOMINGTON_INTAKES.value.replace('0',f"'{etl_date}'")
                elif source in [DataSources.SHELTER_SONOMA,DataSources.SHELTER_NORFOLK] or source.name.startswith("SHELTER_DALLAS"):
                    condition = DateCondition.OTHER.value.replace('0',f"'{etl_date}'")
            response = requests.get(source.value, params={'$limit': limit, '$where':condition})
            result = (source.name.lower(), pd.DataFrame(response.json()))
            if result[1].empty:
                column_names_dict = get_data_columns(DataSources)
                result = (result[0], pd.DataFrame(columns=column_names_dict[result[0]]))
        elif source.name.startswith("POPULATION"):
            response = requests.get(source.value)
            df = pd.read_csv(StringIO(response.text))
            df.columns = ['date', source.name.split('_')[1].title()]
            result = (source.name.lower(), df)
        elif source.name.split('_')[1] == "POPULATION":
            response = requests.get(source.value)
            result = ('_'.join(source.name.split('_')[::-1]).lower(), BeautifulSoup(response.text,'lxml'))
        elif source.name.startswith('PER_CAPITA') or source.name.startswith('UNEMPLOYMENT'):
            response = requests.get(source.value)
            df = pd.read_csv(StringIO(response.text))
            df.columns = ['date', source.name.split('_')[2].title()]
            result = (source.name.lower(), df)         
    except Exception as e:
        log_error_msg(ExtractionErrors.FETCHING_DATA_FROM_SOURCE.value, str(e))
    finally:
        return result


def web_scrape_data(soup):
    table = soup.find_all('table')[0]
    columns = table.find_all('th')
    columns = [column.text.strip() for column in columns]
    df = pd.DataFrame(columns=columns)
    rows = table.find_all('tr')
    for row in rows[1:]:
        row_data = row.find_all('td')
        single_row_data = [data.text.strip() for data in row_data]
        length = len(df)
        df.loc[length] = single_row_data
    df.drop(df.columns[2], axis = 1, inplace=True)
    df.iloc[:,0] = [i.replace('*','').strip() for i in df.iloc[:,0]]
    df.iloc[:,1] = [i.replace(',','').strip() for i in df.iloc[:,1]]
    df = df.astype(np.int64)
    return df


def readData(etl_date = None, limit = None): 
    data_dict = dict()
    try:
        sources = list(DataSources)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda source: fetch_data(source, limit, etl_date), sources))
            for result in results:
                if  result[1] is not None:
                    if isinstance(result[1], pd.DataFrame):
                        data_dict[result[0]] = result[1]
                    elif isinstance(result[1], BeautifulSoup):
                        df = web_scrape_data(result[1])
                        df.columns = ['year', result[0].split('_')[1].title()]
                        data_dict[result[0]] = df
                else:
                    raise Exception(f"{ExtractionErrors.FETCHING_DATA_FROM_SOURCE.value}: a result df is None.")
    except Exception as e:
        log_error_msg(ExtractionErrors.READ_DATA_FN_ERROR.value,str(e))
    finally:
        return data_dict