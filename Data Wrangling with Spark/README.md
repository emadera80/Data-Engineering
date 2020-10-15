# Data Wrangling with Spark

Today I am going to demonstrate data wrangling with spark.   Apache spark is a framework for processing big data.  Spark is like hadoop but but process data 10 times faster.    

I am going to be using Jupyter Notebook to demonstrate how to use spark or PYSPARK.   

The data set I am using a rather small is the FAA bird strike dataset that contains approximately 123K records.  

### Steps 

1.Import all necessary packages: 

Import pyspark 
from pyspark import SparkConf
from pyspark.sql import SparkSession 
import pandas as pd 
import numpy as np 

