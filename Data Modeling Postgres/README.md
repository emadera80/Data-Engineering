## Data Engineering

### Engineering Sparkify data. 

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