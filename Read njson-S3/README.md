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

<b>Problem 2</b>

Since the ndjson_loads() function places the file read into a list of list the patient name and first name were not in the same level for slicing for example: 

created a for loop statement to loop through the list of json code.   in the iteration process i had to slice the data to get information id was straight forward to pull since the id lives in the id field of the json structure. 

for first name and last name in the other had i had to slice further therefore when I was slicing as name[name['given]] as an example it was giving me a key error.  

<b>Solution 2</b>

To fix the key error message I sliced further since I was pulling form a list of list: 

```
name['name'][0]['given'][0]==first_name
name['name'][0]['family']==last_name
```

Using the above snippet I was able to solve the key errors i was encountering.

<b>Problem 3</b>

when counting the id in each resource type file i was looking for ['subject']['reference'] and notice that only a few file return counts. 

<b>Solution 3</b>

I further investigated the resource files and notice that the patient id is set in difference fileds for example ['patient]['reference'] and ['target'] which I loop through to count id.

```
if item['resourceType'] == new:
    if 'patient' in item: 
        if id in item['patient']['reference']: 
            ls.append(new)
        elif 'subject' in item: 
            if id in item['subject']['reference']:
                ls.append(new)
        elif 'target' in item: 
            for x in item['target']: 
                if id in x['reference']:
                    ls.append(new)
```


<b>Other Potential Problem with no Sulotion</b>

From the email recieved with the requirement document and the screenshot stating the expected results this program is not counting for Location, Organization, Practitioner, PractitionerRole.  

The program is counting on ResourceType and ID therefore the patient ID is not listed on Location, Organization, Practitioner and PractitionerRole.  I review the json entries in those file looking for patient information but did not found any.  



### Program Structure

The program have two functions: 

get_patient(): This program will get the patient id, firstname and lastname from the Patient.ndjson file.   

pat_info(): this funciton evaluate the parameters passed on the get_patient the get_patient then passes the values found in the patient.ndjson. the functions then look for the patient id and counts the number of instance the id is preset in each resource file.  


### Running the program 

To run the program in command line you have th option to run it with the patient id or patient name and last name

Command line option 1: 

```
resource.py --firstname Cleo27 --lastname Bode78
```

Comand line option 2: 
```
resource.py --patient_id d13874ec-22ea-46ed-a55c-1fd75ef56a58
```

Running the program in jupyter notebook use the following command 

```
%run resource.py --patient_id d13874ec-22ea-46ed-a55c-1fd75ef56a58
```
or 

```
%run resource.py --firstname Cleo27 --lastname Bode78
```