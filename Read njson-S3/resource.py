import boto3
import pandas as pd 
import configparser 
import ndjson
import re 
from collections import Counter
import sys 
import argparse

config=configparser.ConfigParser() 
config.read_file(open('config.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

s3 = boto3.resource('s3', 
    region_name = 'us-west-2', 
    aws_access_key_id = KEY, 
    aws_secret_access_key = SECRET
)

patient_id = None

def get_patient(id): 

    """
    This function will get patient informaiton from the patient.ndjson file and stored them in variables to be used to find the patient id in other .ndjson file.
    """


    pat = s3.Object('1up-coding-challenge-patients', 'Patient.ndjson')
    pat1 = pat.get()['Body'].read()
    pat2 = ndjson.loads(pat1.decode('utf-8'))


    fnl, lnl, idl = None, None, None

    for name in pat2: 

        if name['id']==patient_id or (name['name'][0]['given'][0]==first_name and name['name'][0]['family']==last_name): 

            fnl = name['name'][0]['given'][0]
            lnl = name['name'][0]['family']
            idl = name['id']    
    
    return fnl, lnl, idl 


def pat_info(patient_id): 

    """
     we use this function to get the information collected from get_patient and count the number of intance the id is populated in the file. 
     the function also write to a data frame to be displayed.
    """
    bucket = s3.Bucket('1up-coding-challenge-patients')

    fn, ln, id = get_patient(patient_id)

    ls = []
    for obj in bucket.objects.all(): 
        filename = []
        filename.append(obj.key)

        if obj.key != 'Patient.ndjson': 

            content = obj

            content1 = content.get()['Body'].read()
            json_data = ndjson.loads(content1.decode('utf-8'))

            for item in filename: 
                bar = re.sub(".ndjson", '', str(item))
                new = bar
                for item in json_data: 
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

    a = Counter(ls).keys()
    b = Counter(ls).values()

    final = pd.DataFrame(list(zip(a,b)), columns = ['RESOURCE_TYPE', 'COUNT'])
    final_df = final.sort_values(['COUNT'], ascending=False)
    return final_df, fn, ln

if __name__=="__main__": 

    parser = argparse.ArgumentParser()
    parser.add_argument("--patient_id")
    parser.add_argument("--firstname")
    parser.add_argument("--lastname")
    args = parser.parse_args()

    patient_id = args.patient_id
    first_name = args.firstname
    last_name = args.lastname

    df, fn, ln = pat_info(patient_id)
    print("Patient Name: {} {}".format(fn, ln))
    print(df)