import pandas as pd 
import boto3 
import json 
import psycopg2 

from botocore.exceptions import ClientError 

import configparser 

def create_iam_role(iam, DWH_IAM_ROLENAME): 
    """
    Create a role for reshift and give permssion to be able to use S3 Services
    """
    try: 
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/', 
            RoleName=DWH_IAM_ROLENAME,
            Description="Allows Redshift cluster to call s3 services",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole', 
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}
                                ], 
                'Version': '2012-10-17'})

        )
    except Exception as e: 
        print(e)

    print("IAM ROLE created")
    print("Attaching Policy")

    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLENAME, 
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
)['ResponseMetadata']['HTTPStatusCode']
    
    print('Policy successfully attached')
    print('Get the IAM role ARN')
    ARN = iam.get_role(RoleName=DWH_IAM_ROLENAME)['Role']['Arn']
    print(ARN)
    print('Iam role ARN retrieved')
    return ARN

def create_cluster(redshift, ARN, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD): 
    """
        Creates the redshift cluster
    """

    try: 
        response=redshift.create_cluster( 
            NumberOfNodes = int(DWH_NUM_NODES), 
            ClusterType = DWH_CLUSTER_TYPE, 
            NodeType = DWH_NODE_TYPE, 
            DBName = DWH_DB, 
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER, 
            MasterUsername = DWH_DB_USER, 
            MasterUserPassword = DWH_DB_PASSWORD,
            IamRoles=[ARN]
        )
    except Exception as e: 
        print(e)

    print('Redshift Cluster Successfully created')


def main(): 

    config = configparser.ConfigParser()
    config.read_file(open('config.cfg'))
    print('config.cfg file successfuly read')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get('DWH', 'dwh_cluster_type' )
    DWH_NUM_NODES = config.get('DWH', 'dwh_num_nodes')
    DWH_NODE_TYPES = config.get('DWH', 'dwh_node_type')
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'dwh_cluster_identifier')
    DWH_DB = config.get('DWH', 'dwh_db')
    DWH_DB_USER = config.get('DWH', 'dwh_db_user')
    DWH_DB_PASSWORD = config.get('DWH', 'dwh_db_password')
    DWH_PORT = config.get('DWH', 'dwh_port')
    DWH_IAM_ROLENAME = config.get('DWH', 'dwh_iam_rolename')


    df = pd.DataFrame({'Param': 
            ['DWH_CLUSTER_TYPE','DWH_NUM_NODES', 'DWH_NODE_TYPE', 'DWH_CLUSTER_IDENTIFIER', 'DWH_DB', 'DWH_DB_USER', 'DWH_DB_PASSWORD', 'DWH_PORT', 'DWH_IAM_ROLENAME'],
            'Value':
            [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPES, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLENAME]
    })

    print('Dataframe for Cluster related properties')
    print(df)

    # AWS Services 

    ec2 = boto3.resource('ec2', 
            region_name = 'us-west-2', 
            aws_access_key_id = KEY, 
            aws_secret_access_key=SECRET 
    )

    s3 = boto3.resource('s3', 
            region_name = 'us-west-2', 
            aws_access_key_id = KEY, 
            aws_secret_access_key=SECRET   
    )

    iam = boto3.client('iam', 
            region_name = 'us-west-2', 
            aws_access_key_id = KEY, 
            aws_secret_access_key=SECRET 
    )

    redshift = boto3.client('redshift', 
            region_name = 'us-west-2', 
            aws_access_key_id = KEY, 
            aws_secret_access_key=SECRET 
    )

    ARN = create_iam_role(iam, DWH_IAM_ROLENAME)

    print('Setting IAM ROLE in config file')
    config.set('IAM_ROLE', "ARN", ARN)

    with open('config.cfg', 'w+') as configfile: 
        config.write(configfile)

    print('Successfully updated the value of ARN in config file')
    create_cluster(redshift, ARN, DWH_CLUSTER_TYPE, DWH_NODE_TYPES, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER,DWH_DB_USER, DWH_DB_PASSWORD)

if __name__ == "__main__": 
    main()