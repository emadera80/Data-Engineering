import pandas as pd
import requests
import io

url = 'https://data.wprdc.org/datastore/dump/5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1'

r = requests.post(url)
if r.ok:
    data = r.content.decode('utf8')
    df = pd.read_csv(io.StringIO(data))