# Import the required lib's
import requests
import json
import ast
import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from transform import transform_data
from pandas import json_normalize
from datetime import datetime, timedelta

# Initiate the process
print()
print("*************************************Search Data ETL process initiate*************************************")
print()

# Compute the previous date
previous_date = datetime.strftime(datetime.now()-timedelta(days=1), "%Y.%m.%d") 
print("Previous date : {}".format(previous_date))


# Define the Elasticsearch index
idx1= "metrics-hotels-f1trm-search-"+str(previous_date)
idx2= "metrics-hotels-f1trm-search"
print("Index name that will be taken from the elastic search : {}".format(idx1))


# Define the message this will be the message defined for the naming the final generated csv files
msg= idx2+str('.csv')


# Define the function for the extract data
def extract_data():

    # Try case
    try:

        # Define the connection string
        # Here the server url is predefined so defined it in static way 
        # If it's dynamic we would have read from the os.getenv()
        es = Elasticsearch("http://172.16.11.54:9800")

        # Define the elastic search index 
        idx=idx1

        # Print the elastic search object
        print()
        print("Elastic Search Host and Port Details  : {}".format(es))

        # Try case for searching the index
        try:
            
            # Define the search and specify the index
            s = Search(using=es, index=idx)
            
            
        # Handle the exception
        except Exception as e:

            # Print the error
            print('Some error occurred while searching the specific index : {}'.format(str(e)))
            print()

        # Search process is compl
        print("Search process is completed going ahead and will be converting into dataframe")
        print()
    
        # Convert into the dataframe
        try:
            # Iterate and convert into the dataframe
            print("Scanning of the file ", s.scan())
            temp = [hit.to_dict() for hit in s.scan()]
            print("Temp ", len(temp))
            
            df = pd.DataFrame(temp)
    
            # Log the information
            print("After converting the search data the dataframe shape is : {}".format(df.shape))
        
        # Handle the exception
        except Exception as e:
            
            # Log the error
            print("Some error occured while converting the fetched record's into the dataframe")
    
        # Calling the transformation logic
        try:

            # Calling the transformation data
            transform_data(df, msg)

        # Handle the exception
        except Exception as e:
            
            # Log the error
            print("Some error occured after calling the transformation logic")
            

    # Handle the exception
    except Exception as e:

        # Print the error
        print('Some error occurred while connecting the elastic search server ={}'.format(str(e)))
        

    
