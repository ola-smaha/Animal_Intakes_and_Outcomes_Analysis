# Reading sources + basic transformation using pandas

import requests
import pandas as pd
from lookups import DataSourcesAPI, DataSourcesIncome
from io import StringIO

# melting in python (might do this on SQL level instead)
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

def readIncomeData():
    income_dict = dict()
    for item in DataSourcesIncome:
        response = requests.get(item.value)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            df.columns = ['date',item.name.split('_')[2].title()]
            income_dict[item.name.split('_')[2].title()] = df
    return income_dict

