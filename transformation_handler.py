# Reading sources + basic transformation using pandas
import requests
import pandas as pd
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from lookups import DataSources, Errors
from logging_handler import show_error_msg

# melting income datasets in python (might do this on SQL level instead, using UNION)
# def readIncomeData():
#     df = pd.DataFrame()
#     for item in DataSourcesIncome:
#         response = requests.get(item.value)
#         if response.status_code == 200:
#             if item.name == DataSourcesIncome.PER_CAPITA_SONOMA_INCOME.name:
#                 df = pd.concat([df,pd.read_csv(StringIO(response.text))])
#                 df.columns = ['date','Sonoma']
#             else:
#                 df[item.name.split('_')[2].title()] = pd.read_csv(StringIO(response.text)).iloc[:,[1]]
#     return pd.melt(df, id_vars=['date'], var_name='region', value_name='personal_income')

def fetch_data(source, limit):
    result = None
    try:
        response = requests.get(source.value, params={'$limit': limit})
        if response.status_code == 200:
            if source.name.startswith("PER_CAPITA"):
                df = pd.read_csv(StringIO(response.text))
                df.columns = ['date', source.name.split('_')[2].title()]
                result = (source.name.lower(), df)
            elif source.name.startswith("SHELTER"):
                result = (source.name.lower(), pd.DataFrame(response.json()))
    except Exception as e:
        show_error_msg(Errors.FETCHING_DATA_FROM_SOURCE.value + f" {source.name}", str(e))
    finally:
        return result

def readData(limit=1):
    income_dict = dict()
    sources = list(DataSources)
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(lambda source: fetch_data(source, limit), sources))
    try:
        for result in results:
            if result is not None:
                income_dict[result[0]] = result[1]
            else:
                print(f"{Errors.FETCHING_DATA_FROM_SOURCE.value}: {result[0]}")
    except Exception as e:
        show_error_msg(Errors.READ_DATA_FN_ERROR.value,str(e))
    finally:
        return income_dict
