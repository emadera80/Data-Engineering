# Project Data Warehouse 

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

For this project I will create a config file that will save related informaiton to Redshift cluster, I AM Role, AWS Keys and S3 bucket. The config file is saved as config.cfg. 

### The structure of the file are as follow: 

1. The create_cluster.py file will create and lauch our cluster in AWS. This program will create a redshift cluster that we will use. 

%run create_cluster.py 

2. After the redshift cluster is aviable we will run the get_cluster_details.py to get cluster information such as host address once the cluster becomes available.   

%run get_cluster_details.py 

3. Run the create_tables.py to create the tables in our redshift cluster.  This creates the necessary tables such as staging table and fact and dimensions tables.   We will use an optimize design using sortkey and distkey.   

%run create_tables.py 

4. Run the ETL.py this program consist of all the necessary steps to load the data into the cluster.  

%run elt.py 

5. Lastly we will use a a program to test the records inserted into our cluster. 

%run test.py 

### Project Structure 

This project contains 6 python files: 

<ul>
<li>create_cluster.py is the program that we use to create a cluster in AWS</li>
<li>create_tables.py is the python file that creates our table in our cluster for this project.</li> 
<li>etl.py this file is what loads the data from S3 into our redshift cluster and process the data to be utilized for analysis and analytics.</li>
<li>sql_queries.py DDL statements that defined the desinged of the table and use in the create_tables.py script. </li>
<li>get_cluster_details.py retrives information of the cluster such as host from the Redshift cluster and updates the config file. </li>
<li>test.py use the sql scripts to count records inserted in the Redshift cluster.</li>

</ul>

### Database Schema Design 

Staging Tabels: 
<ul>
<li>staging_events</li>
<li>staging_sogns</li>

</ul>

Fact table 

Songplays - this records are event data associated to song plays for example records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent. 

Dimension Tables

users - users in the app - user_id, first_name, last_name, gender, level
songs - songs in music database - song_id, title, artist_id, year, duration
artists - artists in music database - artist_id, name, location, lattitude, longitude
time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday

### Query Results 

    SELECT COUNT(*) FROM staging_events
 returns 8056 records

    SELECT COUNT(*) FROM staging_songs
 returns 14896 records

    SELECT COUNT(*) FROM sonplays
 returns 333 records

    SELECT COUNT(*) FROM users
 returns 104 records

    SELECT COUNT(*) FROM songs
 returns 14896 records

    SELECT COUNT(*) FROM artists
 returns 10025 records

    SELECT COUNT(*) FROM time
 returns 333 records