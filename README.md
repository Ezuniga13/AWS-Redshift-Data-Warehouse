# AWS-Redshift-Data-Warehouse
A redshift cluster with staging tables and final tables ready for the analytics team to work.

Esteban Zuniga <br>
September 23, 2022 <br>
Data Engineer

## Overview

Sparkify analytics team is interested in understanding what songs users are listening to on their music stream app. However, they don't
have an easy way to query their data that resides in a directory of json logs on user acitivity and metadata on the songs.

I've created a data warehouse with tables designed to optimize queries on songplay analysis. I've also built an ETL pipeline using python, SQL, IAM, EC2, s3, AWD sdk boto3 and IaC
that transfers data from files in two local directories into these tables.

I've defined fact(songplays) and dimension(users, songs, time, artists) tables for a star schema.


### How to run the Pyton scripts


**Steps**

1. Bring the all the files in your root directory.
2. In the command terminal move into the root directory.
3. Open the IaC.ipynb file and follow intructions to invoke Iam roles, s3 etc and to attach policies. STOP at 2.2 after creating redshift cluster
4. Run create_tables.py with this command -- python create_tables.py
4. To test if tables have been created got to the aws console in redshift and connect to the db and check to see if tables were created.
5. Go back to IaC notebook and run the EC2 cell to open up the TCP port STOP there 
6. run etl.py with this command -- python etl.py IMPORTANT this can take up to 10-15 minutes a piece to load. The data has to be loaded into staging tables then insersted into the star schema
7. to test if the data has been insersted go back to redshift and use the query editor and run some queries. 
8. Once the ETL job is done go back to IaC notebook and delete your cluster and roles. 

### Files in the repository

- sql_queries.py <br>
  This script contains all SQL queries, create and insert     statements saved in python variables.
  You will not need to run this script but it needs to be in the same directory that create_tables.py is because we will be importing modules 
  from it.

- create_tables.py <br>
    This python script has three purposes.
    1. To connect to redshift cluster and create a database
    2. To drop any tables that were created by before
    3. To create tables that will take in our values
    
- IaC.ipynb
    This notebook is probably the most import file here because you are using AWS sdk boto3 to create/delete/interactive with many tools such as IAM, EC2 ... etc. You must follow in sequence and have the AWS console open to monitor your code is running correctly. It is very important to stop and run the other python scripts when the notes instruct you to do so. When finished delete your resources, else you will be charged. 

    
-  etl.py
    This is the actual etl pipeline that processes our song and log files into working pipelines that transfers data into our tables in the cluster for analysis. The main function of this script is to BULK copy raw data that resides in s3 buckets into staging tables. This may take time but more professional than inserting row by row. Once the data has been loaded into staging tables it will be transformed and loaded into the star schema and olap cubes can be easily created from here. And analyst can query directly with SQL here. 

- dwh.cfg
  This file is where you will update your secrets and arn's as you progress through the IaC notebook. 



### Star Schema Design and ETL pipelinee

The schema is designed for the analytics team to get insights on what songs their users are listening to and to improve performance by reducing the number of joins.

The fact table labeled as songplays has fields that are comprised from the primary keys for the outer tables.

The outer tables have been denormalized and have user, song, artist and time information that has been duplicated.

