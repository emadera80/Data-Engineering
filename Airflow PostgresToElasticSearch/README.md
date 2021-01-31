# Creating a data pipeline. 

In this project i used Faker to create the dummy data and stored it in a postgres table.  

the postgrestable.py script contains the code that creates the dummy data and the postgress implementation.   

Couple of requirements I am using ubuntu wsl for windows 10 i have installed elasticsearch database and ran the service to get the data started and also postgress service.   also ran the airflow webserver on port 8081 and airflow scheduler.  

Created my airflow dag using code DagPGElas.py which will call the data and query it in postgress and loaded into elasticsearch. 