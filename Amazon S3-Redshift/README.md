# Moving Data From S3 To Redshift

In this example I am going to demonstrate how to move data from an S3 bucket to amazon redshift cluster. 

The steps. 

Using the following python package i will import them to iteract with AWS I will use boto3: 

boto3 
pandas 
json 

I will create a config file in which I need the python configparser package to pull information that I am going to use to connect into AWS.  

Create a AWS Redshift Cluster
Create and Assign a ARN Role 
Connect to the cluster 
Connect to S3 
Design a data model 
Copy the data from S3 to the redshift model.   