# Import the required lib's
import pandas as pd
import json
from pandas import json_normalize
import ast
from post_transform import post_transformation


# Define the function for the flattening the json data
def only_dict(d):
   
    #Convert json string representation of dictionary to a python dict
    return ast.literal_eval(str(d))


# Define the transformation data method for transforming the data
def transform_data(df,msg):

    print()

    # Log the debugging statement
    print("In the transformation method")

    # Log the incoming dataframe 
    print("Size of the incoming dataframe  : {}".format(df.shape))

    # Log the columns of the dataframe
    print("Columns of the incoming dataframe  : {}".format(df.columns))

    # Ignore the warnings
    pd.options.mode.chained_assignment = None 
    
    # Convert it into the pandas datetime
    fd1 = df[['@timestamp', 'data']]
    fd1['@timestamp'] = pd.to_datetime(fd1['@timestamp']).dt.tz_localize(None)
    fd1['@timestamp'] = fd1['@timestamp'].apply(lambda x: x.replace(microsecond=0))

    print("In transform after processing the timestamp field colums as follows=\n{}".format(df.columns))
    print()

    # Flatten the dict fields from the dataframe
    try:

        # Normalize the dataframe and extract the json & store it in individual column
        df1= json_normalize(df['data'].apply(only_dict).tolist()).add_prefix('')

        df1['time_stamp'] = fd1['@timestamp']

        # Print the normalized dataframe columns
        print("Flatten dataframe columns ={}".format(df1.columns))
        print()

        # Finally log the information of the process completion
        print("Going to call the post transformation method...")
        print()

        # Try case
        try:
            
            # Sending the dataframe for the post_transformation
            post_transformation(df1,msg)

        # Handle the excception
        except Exception as e:

            # Log the information
            print("Some error occured after returning from the post transformation")

    # Handle the exception 
    except Exception as e:

        # Log the information
        print("Some exception occured ={}".format(str(e)))
        
    