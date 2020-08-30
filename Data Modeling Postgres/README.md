## Data Engineering

### Engineering Sparkify data. 

This is the my project submission for the Data Engineering. This project is built using below concepts:
<u>
<li>Data modeling with Postgres</li>
<li>Star Schema</li>
<li>ETL using Python</li>
</u>

### Context

Sparkify is a startup music app company that have been collecting data on songs and user activity in their app, currently the analytics team are interested in analyzing the data to understand their user base.  At the present tme there the analytical team dont have a good or easy way to query the data which redise in json format.  

In this project I'll apply what I've learned on data modeling with Postgres and build an ETL pipeline using Python.

### Data
Song datasets: all json files are nested in subdirectories under /data/song_data. A sample of this files is:
```
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
Log datasets: all json files are nested in subdirectories under /data/log_data. A sample of a single row of each files is:
```
    {"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level"
```

### Project structure

Files used on the project:

    1. data folder contains Jason files to be processed.
    2. create_tables.py drops and creates tables. Running this script will create the database and tables. 
    3. sql_queries.py this script contains all the sql code that makes up the database and tables.  It also contains the insert statements that etl.py will use to insert the data.
    4. test.ipynb allows to check what is being inserted in the database..
    5. etl.ipynb enables to process single rows of data for effectively laying the programming foundation of the etl process.
    6. etl.py ETL program is the final programs that contains the logic for data model.
    7. README.md prodiveds steps and information on this project.

### Database Schema

Star Schema was modeled for this requirement: There is one main fact table and 4 dimentional tables, each with a primary key that is being referenced from the fact table.

Relational data model was choseng for this  project as it is beneficial for the end user to write queries for their analytical endeavors.

Fact Table
songplays - log data records of song played by users i.e. records with page NextSong

Dimension Tables
users - users in the app

songs - songs in music database

artists - artists in music database

time - timestamps of records in songplays broken down into specific units


Author: Enmanuel Madera 
date: 8/28/2020
Follow me on LinkedIn