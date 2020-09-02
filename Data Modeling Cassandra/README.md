# Data Modeling with Cassandra

### Project Scope

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.


### Project steps. 

Created a csv file that contains the combined csv in the dictory envent_data  I will utilize that csv file to load into cassandra.  

Used Python to create the master CSV file that will contain our data to load into cassandra. 

After creating the master csv file Connected to cassandra by importing the from cassandra.cluster import Cluster and create a cluster to connect to 127.0.0.1.  

create a key space this is the database we are creating in cassandra. we used CREATE KEYSPACE IF NOT EXISTS and gave it sparkfy name as the project instruction suggested. with used replication with simple strategy.   After creating the keyspace set the keyspace session and set the keyspace to sparkify. 

To keep code clean and consistant create two function to call query the data. 

1. execute_query funciton will be used to select data from cassandra key space. 

2. excute_query_params will be used to insert data into cassandra.  

Create three tables in cassandra: 
session_library this table will be used primarily to pull session information and it contains a composite index on session_id and item_in_session 

user_library this table will be used to store user information with primary key on user_id, session_id and clustered on item_in_session 

user_song_library will be used to store user and song information with index on song, and user_id. 

after creating tables we then use python and the fucntions we created to insert the data and query the data based on the use case provider on the project page.  
