# Reading sources + basic transformation using pandas
import requests
import pandas as pd
import numpy as np
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from lookups import DataSources, TransformationErrors, StagingTablesNames, DateCondition
from logging_handler import log_error_msg
import warnings
from bs4 import BeautifulSoup

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
        log_error_msg(TransformationErrors.FETCHING_DATA_FROM_SOURCE.value, str(e))
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
    income_dict = dict()
    try:
        sources = list(DataSources)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda source: fetch_data(source, limit, etl_date), sources))
            for result in results:
                if  result[1] is not None:
                    if isinstance(result[1], pd.DataFrame):
                        income_dict[result[0]] = result[1]
                    elif isinstance(result[1], BeautifulSoup):
                        df = web_scrape_data(result[1])
                        df.columns = ['year', result[0].split('_')[1].title()]
                        income_dict[result[0]] = df
                else:
                    print(f"{TransformationErrors.FETCHING_DATA_FROM_SOURCE.value}: a result df is None.")
    except Exception as e:
        log_error_msg(TransformationErrors.READ_DATA_FN_ERROR.value,str(e))
    finally:
        return income_dict


def clean_sonoma_dataset(dfs):
    sonoma = None
    try:
        df = dfs['shelter_sonoma']   
        sonoma = df.copy()
        sonoma = sonoma.drop_duplicates()
        date_columns = ['intake_date','date_of_birth','outcome_date']
        sonoma[date_columns] = sonoma[date_columns].apply(pd.to_datetime)
        sonoma.dropna(subset=['intake_date','intake_type'], inplace=True)
        sonoma['outcome_type'] = sonoma['outcome_type'].fillna('Pending')
        sonoma['id'] = sonoma['id'].apply(lambda x: f'SO-{x}')
        data_columns = ['type', 'breed', 'color', 'sex', 'intake_type', 'outcome_type']
        sonoma[data_columns] = sonoma[data_columns].apply(lambda x: x.str.title())
        sonoma['region'] = 'Sonoma'
        sonoma.loc[sonoma['outcome_type'] == 'Rtos', 'outcome_type'] = 'Return To Owner'
        sonoma['color'] = sonoma['color'].replace({'Bl ': 'Black ', 'Brn ' : 'Brown '},regex=True)
        sonoma.rename(columns = {'id':'animal_id'},inplace = True)
    except Exception as e:
        log_error_msg(TransformationErrors.CLEAN_SONOMA_DF_ERROR.value,str(e))
    finally:
        return sonoma

def clean_austin_datasets(dfs):
    combined_df = None
    try:
        df1 = dfs['shelter_austin_intakes']
        df2 = dfs['shelter_austin_outcomes']
        austin_in = df1.copy()
        austin_out = df2.copy()
        austin_in.rename(columns={'animal_type':'type','datetime':'intake_date'},inplace=True)
        austin_out.rename(columns={'animal_type':'type','datetime':'outcome_date','sex_upon_outcome':'sex'},inplace=True)
        austin_in.sort_values(by='intake_date', inplace=True)
        austin_out.sort_values(by= 'outcome_date', inplace=True)
        austin_in['intake_date'] = pd.to_datetime(austin_in['intake_date'])
        date_columns = ['date_of_birth','outcome_date']
        for col in date_columns:
            austin_out[col] = pd.to_datetime(austin_out[col])
        austin_in.drop_duplicates(subset='animal_id', keep='first', inplace=True)
        austin_out.drop_duplicates(subset='animal_id', keep='first', inplace=True)
        combined_df = pd.merge(austin_in, austin_out, on='animal_id', how = 'left')
        combined_df['outcome_type'] = combined_df['outcome_type'].fillna('Pending')
        combined_df['sex'].replace({'NULL': 'Unknown', 'Intact Male': 'Male', 'Intact Female': 'Female', 'Neutered Male': 'Neutered', 'Spayed Female': 'Spayed'}, inplace=True)
        combined_df['type'] = np.where(~combined_df['type'].isin(['Cat', 'Dog']), 'Other', combined_df['type'])
        combined_df['animal_id'] = combined_df['animal_id'].apply(lambda x: f'AUS-{x}')
        combined_df['region'] = 'Austin'
        combined_df.outcome_type.replace({'Relocate':'Transfer', 'Rto-Adopt': 'Return to Owner', 'Euthanasia':'Euthanize'},inplace=True)
    except Exception as e:
        log_error_msg(TransformationErrors.CLEAN_AUSTIN_DF_ERROR.value,str(e))
    finally:
        return combined_df

def clean_norfolk_dataset(dfs):
    norfolk = None
    try:
        df = dfs['shelter_norfolk']
        norfolk = df.copy()
        norfolk['years_old'] = pd.to_numeric(norfolk['years_old'])
        norfolk['months_old'] = pd.to_numeric(norfolk['months_old'])
        norfolk['intake_date'] =  pd.to_datetime(norfolk['intake_date'])
        norfolk['outcome_date'] =  pd.to_datetime(norfolk['outcome_date'])
        age_in_days = np.where(
            (~norfolk['years_old'].isna()) | (~norfolk['months_old'].isna()),
            ((norfolk['years_old'].fillna(0) * 12 + norfolk['months_old'].fillna(0)) * 30),
            np.nan)
        warnings.filterwarnings("ignore", category=RuntimeWarning, module="pandas.core.arrays.timedeltas")
        norfolk['date_of_birth'] = norfolk['intake_date'] - pd.to_timedelta(age_in_days, unit='D')
        norfolk.drop(['years_old','months_old'],axis=1, inplace=True)
        norfolk['date_of_birth'] =  pd.to_datetime(norfolk['date_of_birth'])
        norfolk.rename(columns={'animal_type':'type','primary_breed': 'breed', 'primary_color':'color'},inplace=True)
        norfolk.drop_duplicates(inplace=True)
        norfolk['type'] = np.where(~norfolk['type'].isin(['Cat', 'Dog']), 'Other', norfolk['type'])
        data_columns = ['breed', 'color']
        norfolk[data_columns] = norfolk[data_columns].apply(lambda x: x.str.title())
        norfolk['color'] = norfolk['color'].replace({'Bl |Blk ': 'Black ', 'Brn |Br ' : 'Brown ', 'Org':'Orange', 'Sl ':'Silver '},regex=True)
        norfolk['sex'].replace({'NULL': 'Unknown', 'Intact Male': 'Male', 'Intact Female': 'Female', 'Neutered Male': 'Neutered', 'Spayed Female': 'Spayed'}, inplace=True)
        norfolk.reset_index(drop=True, inplace=True)
        norfolk.intake_type.replace({'Owner Surrendered':'Owner Surrender','Confiscated':'Confiscate'},inplace=True)
        norfolk['outcome_type'] = norfolk['outcome_type'].fillna('Pending')
        norfolk.outcome_type.replace({'Euthanized':'Euthanize','Disposal of Deceased Pet':'Disposal','Unassisted Death':'Died'},inplace=True)
        norfolk['animal_id'] = norfolk['animal_id'].apply(lambda x: f'NOR-{x}')
        norfolk['region'] = 'Norfolk'
    except Exception as e:
        log_error_msg(TransformationErrors.CLEAN_NORFOLK_DF_ERROR.value,str(e))
    finally:
        return norfolk

def age_to_days(age):
    years, months, weeks = 0, 0, 0
    parts = age.split()
    i = 0
    while i < len(parts):
        if parts[i].isdigit():
            value = int(parts[i])
            if i + 1 < len(parts) and 'year' in parts[i + 1]:
                years = value
            elif i + 1 < len(parts) and 'month' in parts[i + 1]:
                months = value
            elif i + 1 < len(parts) and 'week' in parts[i + 1]:
                weeks = value
        i += 1
    return years * 365 + months * 30 + weeks * 7

def clean_bloomington_dataset(dfs):
    bloomington = None
    try:
        df = dfs['shelter_bloomington']
        bloomington = df.copy()
        bloomington.rename(columns={'speciesname':'type','breedname': 'breed', 'basecolour':'color','sexname':'sex','intakedate':'intake_date','movementdate':'outcome_date','intakereason':'intake_type','movementtype':'outcome_type'},inplace=True)
        bloomington['intake_date'] =  pd.to_datetime(bloomington['intake_date'])
        bloomington['outcome_date'] =  pd.to_datetime(bloomington['outcome_date'])
        bloomington.drop_duplicates(inplace=True)
        bloomington['age_in_days'] = bloomington['animalage'].apply(age_to_days)
        bloomington = bloomington[~(bloomington['age_in_days'] == 355385)]
        bloomington.loc[:, 'date_of_birth'] = bloomington['intake_date'] - pd.to_timedelta(bloomington['age_in_days'], unit='D')
        bloomington.drop(['animalage','age_in_days'],axis=1, inplace=True)
        bloomington.reset_index(drop=True, inplace=True)
        bloomington['id'] = bloomington['id'].apply(lambda x: f'BL-{x}')
        bloomington['region'] = 'Bloomington'
        bloomington.loc[~bloomington['type'].isin(['Cat', 'Dog', 'Bird', 'Livestock','Other']), 'breed'] = bloomington['type']
        bloomington['type'] = np.where(~bloomington['type'].isin(['Cat', 'Dog']), 'Other', bloomington['type'])
        bloomington['intake_type'].replace({'Transfer from Other Shelter':'Transfer','Abuse/ neglect':'Confiscate', 'Owner requested Euthanasia':'Euthanasia Request','DOA':'Disposal Request of Deceased Pet'},inplace = True)
        bloomington['outcome_type'].replace({'Reclaimed':'Return to Owner','Released To Wild':'Returned to Native Habitat','None':'Pending'},inplace = True)
        values_to_replace = ['Incompatible with owner Lifestyle', 'Moving', 'Unsuitable Accommodation', 'Unable to Afford', 'Allergies', 'Incompatible with other pets', 'Marriage/Relationship split']
        for value in values_to_replace:
            bloomington['intake_type'].replace(value, 'Owner Surrender', inplace=True)
    except Exception as e:
        log_error_msg(TransformationErrors.CLEAN_BLOOMINGTON_DF_ERROR.value,str(e))
    finally:
        return bloomington

def clean_dallas_dataset(dfs):
    dallas = None
    try:
        dallas_merged = pd.DataFrame()
        for key, df in dfs.items():
            if 'shelter_dallas' in key:
                dallas_merged = pd.concat([dallas_merged, df])
        dallas = dallas_merged.copy()
        dallas.drop_duplicates(inplace=True)
        dallas.rename(columns={'animal_type':'type','animal_breed':'breed'},inplace=True)
        dallas['intake_date'] = pd.to_datetime(dallas['intake_date'])
        dallas['intake_time'] = pd.to_timedelta(dallas['intake_time'])
        dallas['outcome_date'] = pd.to_datetime(dallas['outcome_date'])
        dallas['outcome_time'] = pd.to_timedelta(dallas['outcome_time'])
        dallas['intake_date'] = dallas['intake_date'].dt.normalize() + dallas['intake_time']
        dallas['outcome_date'] = dallas['outcome_date'].dt.normalize() + dallas['outcome_time']
        dallas.drop(['intake_time','outcome_time'],axis=1, inplace=True)
        data_columns = ['type','breed','intake_type','outcome_type']
        dallas[data_columns] = dallas[data_columns].apply(lambda x: x.str.title())
        dallas['type'] = np.where(~dallas['type'].isin(['Cat', 'Dog']), 'Other', dallas['type'])
        dallas['intake_type'].replace({'Confiscated':'Confiscate'},inplace = True)
        dallas['outcome_type'].replace({'Euthanized':'Euthanize','Returned To Owner':'Return To Owner','Dead On Arrival':'Disposal'},inplace = True)
        dallas.reset_index(drop=True, inplace=True)
        dallas['animal_id'] = dallas['animal_id'].apply(lambda x: f'DAL-{x}')
        dallas['region'] = 'Dallas'
        dallas['sex'] = dallas['color'] = None
        dallas['date_of_birth'] = pd.NaT
    except Exception as e:
        log_error_msg(TransformationErrors.CLEAN_DALLAS_DF_ERROR.value,str(e))
    finally:
        return dallas

# def expand_dataframe(df):
#     try:
#         df['date'] = pd.to_datetime(df['date'])
#         columns = df.columns
#         expanded_data = []
#         for i in range(len(df) - 1):
#             current_row = df.iloc[i]
#             next_row = df.iloc[i + 1]
#             growth_rate = (next_row[1] - current_row[1])
#             expanded_data.append([current_row[0], round(current_row[1], 3)])
#             current_date = current_row[0]
#             while current_date < next_row[0]:
#                 current_date += pd.DateOffset(months=1)
#                 expanded_data.append([current_date, round(expanded_data[-1][1] + growth_rate / 12, 3)])
#         expanded_df = pd.DataFrame(expanded_data, columns=columns)
#     except Exception as e:
#         log_error_msg(TransformationErrors.EXPAND_DF.value,str(e))
#     return expanded_df

def transform_unemployment_data(df):
    try:
        if 'date' in df:
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].dt.year
        annual_unemployment_df = df.groupby('year')[df.columns[1]].mean().reset_index()
    except Exception as e:
        log_error_msg(TransformationErrors.TRANSFORM_UNEMPLOYMENT_DATA.value, str(e))
    finally:
        return annual_unemployment_df
    

def clean_all_data(dfs):
    clean_data_dict = {}
    try:
        sonoma = clean_sonoma_dataset(dfs)
        austin = clean_austin_datasets(dfs)
        norfolk = clean_norfolk_dataset(dfs)
        bloomington = clean_bloomington_dataset(dfs)
        dallas = clean_dallas_dataset(dfs)
        intakes_dfs = [sonoma,austin,norfolk,bloomington,dallas]
        object_columns = ['type', 'breed', 'color', 'intake_type', 'sex', 'outcome_type']
        date_columns = ['date_of_birth','intake_date','outcome_date']
        for df in intakes_dfs:
            df[object_columns] = df[object_columns].astype(str)
            df[date_columns] = df[date_columns].fillna('2262-04-11').astype('datetime64[ns]')
        clean_data_dict.update({f"{StagingTablesNames.SONOMA_INTAKES_OUTCOMES.value}":sonoma,
                                f"{StagingTablesNames.AUSTIN_INTAKES_OUTCOMES.value}":austin,
                                f"{StagingTablesNames.NORFOLK_INTAKES_OUTCOMES.value}":norfolk,
                                f"{StagingTablesNames.BLOOMINGTON_INTAKES_OUTCOMES.value}":bloomington,
                                f"{StagingTablesNames.DALLAS_INTAKES_OUTCOMES.value}":dallas})
        for key,value in dfs.items():
            if key.startswith("unemployment"): 
                    annual_df = transform_unemployment_data(value)
                    clean_data_dict.update({key:annual_df})
            elif key.startswith("population") or key.startswith("per_capita"):
                if value.columns[0] == 'date':
                    value['date'] = pd.to_datetime(value['date'])
                    value['date'] = value['date'].dt.year
                    value.rename(columns={'date':'year'}, inplace = True)
                    clean_data_dict.update({key:value})
                else:
                    clean_data_dict.update({key:value})
    except Exception as e:
            log_error_msg(TransformationErrors.CLEAN_ALL_DATA.value, str(e))
    finally:
        return clean_data_dict
    