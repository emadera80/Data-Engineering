import psycopg2 as pg 
from faker import Faker 

fake = Faker()

data=[] 

i = 1 

for r in range (1000): 
    data.append((i, fake.name(), fake.street_address(), fake.city(), fake.zipcode()))
    i += 1 

data_for_db = tuple(data) 

print(data_for_db)

conn = pg.connect("host=127.0.0.1 port=5432 dbname=maderaanalytics user=manny password=Yankees1")
cur = conn.cursor()

cur.execute("""
    drop table if exists users; 

    create table users (

        id  int primary key, 
        name text, 
        address text, 
        city text, 
        zipcode int
    )
""")

query = "insert into users (id, name, address, city, zipcode) values (%s, %s, %s, %s, %s)"

cur.executemany(query, data_for_db)

conn.commit() 

query2 = "select * from users" 

cur.execute(query2)

print(cur.fetchall())

conn.close()



