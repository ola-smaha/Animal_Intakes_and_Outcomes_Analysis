from enum import Enum

class Errors(Enum):
    FETCHING_DATA_FROM_SOURCE = "Error fetching data from source"
    READ_DATA_FN_ERROR = "Error processing results"

class DataSources(Enum):
    SHELTER_SONOMA = "https://data.sonomacounty.ca.gov/resource/924a-vesw.json?$select=id,type,breed,color,sex,date_of_birth,intake_date,outcome_date,intake_type,outcome_type"
    SHELTER_AUSTIN_INTAKES = "https://data.austintexas.gov/resource/wter-evkm.json?$select=animal_id,animal_type,breed,color,sex_upon_intake,age_upon_intake,datetime,intake_type"
    SHELTER_AUSTIN_OUTCOMES = "https://data.austintexas.gov/resource/9t4d-g238.json?$select=animal_id,sex_upon_outcome,age_upon_outcome,date_of_birth,datetime,outcome_type"
    SHELTER_NORFOLK = "https://data.norfolk.gov/resource/vfm4-5wv6.json?$select=animal_id,animal_type,primary_breed,primary_color,sex,years_old,months_old,intake_date,outcome_date,intake_type,outcome_type"
    SHELTER_BLOOMINGTON = "https://data.bloomington.in.gov/resource/e245-r9ub.json?$select=id,speciesname,breedname,basecolour,sexname,animalage,intakedate,movementdate,intakereason,movementtype"
    SHELTER_DALLAS_2017_2018 = "https://www.dallasopendata.com/resource/wb7n-sdxi.json?$select=animal_id,animal_type,animal_breed,intake_date,intake_time,outcome_date,outcome_time,intake_type,outcome_type"
    SHELTER_DALLAS_2018_2019 = "https://www.dallasopendata.com/resource/kf5k-aswg.json?$select=animal_id,animal_type,animal_breed,intake_date,intake_time,outcome_date,outcome_time,intake_type,outcome_type"
    SHELTER_DALLAS_2019_2020 = "https://www.dallasopendata.com/resource/7h2m-3um5.json?$select=animal_id,animal_type,animal_breed,intake_date,intake_time,outcome_date,outcome_time,intake_type,outcome_type"
    SHELTER_DALLAS_2020_2021 = "https://www.dallasopendata.com/resource/sq59-vp2t.json?$select=animal_id,animal_type,animal_breed,intake_date,intake_time,outcome_date,outcome_time,intake_type,outcome_type"
    SHELTER_DALLAS_2021_2022 = "https://www.dallasopendata.com/resource/uu3b-ppfz.json?$select=animal_id,animal_type,animal_breed,intake_date,intake_time,outcome_date,outcome_time,intake_type,outcome_type"
    SHELTER_DALLAS_2022_2023 = "https://www.dallasopendata.com/resource/f77p-sgrc.json?$select=animal_id,animal_type,animal_breed,intake_date,intake_time,outcome_date,outcome_time,intake_type,outcome_type"
    # CSV download button links (using inspect)
    PER_CAPITA_SONOMA_INCOME = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1138&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=PCPI06097&scale=left&cosd=1969-01-01&coed=2021-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-10-07&revision_date=2023-10-07&nd=1969-01-01"
    PER_CAPITA_AUSTIN_INCOME = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1138&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=PCPI48015&scale=left&cosd=1969-01-01&coed=2021-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-10-07&revision_date=2023-10-07&nd=1969-01-01"
    PER_CAPITA_NORFOLK_INCOME = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1138&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=PCPI51710&scale=left&cosd=1969-01-01&coed=2021-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-10-07&revision_date=2023-10-07&nd=1969-01-01"
    PER_CAPITA_BLOOMINGTON_INCOME = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1138&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=BLOO018PCPI&scale=left&cosd=1969-01-01&coed=2021-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-10-07&revision_date=2023-10-07&nd=1969-01-01"
    PER_CAPITA_DALLAS_INCOME = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1138&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=PCPI48113&scale=left&cosd=1969-01-01&coed=2021-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2023-10-07&revision_date=2023-10-07&nd=1969-01-01"