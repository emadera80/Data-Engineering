import csv 

with open('data.csv', 'r') as f: 
    myreader=csv.DictReader(f)
    headers = next(myreader)
    for row in myreader: 
        print(row['name'])