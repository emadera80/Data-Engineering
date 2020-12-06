# Code Challenge

### Packages Needed

To successfully run the program you will need to import the following packages.  

You will need your Amazon Access Key and Secrete Key to be able to run the program in command line You can also run the program in notebook as well using the following command %run resource.py --patient_id d13874ec-22ea-46ed-a55c-1fd75ef56a58

```
import boto3
import pandas as pd 
import configparser 
import ndjson
import re 
from collections import Counter
import sys 
import argparse
```
Using the boto3 we can create a connection to the S3 client using the below code snippet.  

To collect the access key I have created a config file that will be hidden to prevent unauthorized user the ability to access my Amazon key.  Hence for this config file will 
not have the access key and secret key on the submission.  The end user must provide their own Access key and secret key in the config file.

```
s3 = boto3.resource('s3', 
    region_name = 'us-west-2', 
    aws_access_key_id = KEY, 
    aws_secret_access_key = SECRET
)

```

Our S3 bucket that we will be looking for out files is located in the following S3 location: s3://1up-coding-challenge-patients

In python we use the following code to connect to the bucket and view the contents of the bucket 

```
bucket = s3.Bucket('1up-coding-challenge-patients')

for obj in bucket.objects.all():
    print(obj)
```
Here is the results of he returned objects from the above code. 

```
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='AllergyIntolerance.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='CarePlan.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='CareTeam.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Claim.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Condition.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Device.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='DiagnosticReport.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='DocumentReference.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Encounter.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='ExplanationOfBenefit.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Group.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='ImagingStudy.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Immunization.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Location.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Medication.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='MedicationAdministration.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='MedicationRequest.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Observation.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Organization.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Patient.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Practitioner.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='PractitionerRole.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Procedure.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='Provenance.ndjson')
s3.ObjectSummary(bucket_name='1up-coding-challenge-patients', key='SupplyDelivery.ndjson')
```

### Problem encounter

<b>Problem 1</b>
During the analysis of the file I encountered a few problems with reading the ndjson file from S3.   I was using the json package to read in the file in wich i was unsucessfull in loading. it gave me the following error: JSONDecodeError: Extra data: line 2 column 1 (char 3913)

the code I ran at first was:

```
pat = s3.Object('1up-coding-challenge-patients', 'Patient.ndjson')
pat1 = pat.get()['Body'].read()
pat2 = json.loads(pat1.decode('utf-8'))

```

<b>Solution 1</b>
After researching on stackoverflow I end up pip install ndjson to process the ndjson files.   The fixed the code as follows: 

```
pat = s3.Object('1up-coding-challenge-patients', 'Patient.ndjson')
pat1 = pat.get()['Body'].read()
pat2 = ndjson.loads(pat1.decode('utf-8'))

```
The above ran and read successfully with no errors.  



### Program detail

The program have two functions: 

get_patient() will get the patient information from the patient file we then store the patient first name, last name and patient id into variables and use them in the return statement to be ble to use them in the pat_info() 

To be able to get patient informaiton user must run the py program with the following variables --firstname and -- lastname or --patient_id.  The program will search for the the inputted information in the patient file and stored the findings in variable in returned them.  