import pandas as pd 
import boto3
import json 
import psycopg2 

from botocore.exceptions import ClientError 
import configparser

config  = configparser.ConfigParser() 
config.read_file(open('config.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'dwh_cluster_identifier')
DWH_IAM_ROLE = config.get('DWH', 'dwh_iam_rolename')

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "NumberOfNodes"]
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


iam = boto3.client('iam',
        region_name = 'us-west-2', 
        aws_access_key_id = KEY, 
        aws_secret_access_key = SECRET
)

redshift = boto3.client ('redshift',
        region_name = 'us-west-2', 
        aws_access_key_id = KEY, 
        aws_secret_access_key = SECRET

)


#deleting cluster to clear resources 

redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

iam.detach_role_policy(RoleName=DWH_IAM_ROLE, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE)