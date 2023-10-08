import requests
from bs4 import BeautifulSoup

req = requests.get('https://usafacts.org/data/topics/people-society/population-and-demographics/population-data/population/#chart-12781-5')
soup = BeautifulSoup(req.content, "html.parser")
print(soup.title.prettify())




