## Data Engineering

### Engineering Sparkify data. 

Author: Enmanuel Madera 
date: 8/28/2020


Created sparkyfidb to stored song data.  Currently analytics team do not have a mean to query the data as the data resides on a network location as json format.   

Create 5 tables in postgres to store the data that currently is json format.

Tables:
Songplays - contains information of the songs that were played by the app user
Songs - contains list of songs in the app
Artists - contains list of artists in the app
Time - contains time for time analysis of the data used by the app. 
Users - users that are register to the app. 