# Import the required lib's
import numpy as np
import pandas as pd
import os

# Define the csv storage file path
dest_path = "./etl_csv_files"
print("Destination file path ={}".format(dest_path))



# Write the mapped function for the search_type
def type_mapper(key):
    
    if key in ['HOTEL_DETAIL_SEARCH']:
        
        # Return HDS
        return "HDS"
        
     
    elif key in ['CITY_SEARCH']:
        
        # Return CS
        return "CS"
        
        
    elif key in ['REVIEW_PAGE']:
        
        # Return RP
        return "RP"
    
    elif key in ['CITY_SEARCH_PAGINATION']:
        
        # Return CSP
        return "CSP"
    
    else :
        
        # Return Not mapped
        return 'Not Mapped'



# Write the mapped function for the api_key
def mapper(key):
    
    if key in ['SM08QZRiCkBRiXDToujw']:
        
        return "1"
        #return 'Corporate (Agency model)'
     
    elif key in ['Nwcp6g6BTACcBbZ3USlZ']:
        
        return "2"
        #return 'Corporate (Reseller)'
     
    elif key in ['ouKiY2m0sre8n0z4KeUS']:
        
        return "3"
        #return 'B2B domestic'
      
    elif key in ['84F8B7D17824E82C6643']:
        
        return "4"
        #return 'ATB'
    
    elif key in ['8B1E4165834349BFC103']:
        
        return "5"
        #return 'B2B International'
     
    elif key in ['v91DmEGMtQEAj38KiGBy', 'c26YRgHsFpPb8xqFs2VY']:
        
        return "6"
        #return 'Corporate International'
     
    elif key in ['RHyUcLJ9OYSYwMDF3M5a']:
        
        return "7"
        #return 'Atb International Corporate'
 
    elif key in ['3A57560EE310C5899DCB']:
        
        return "8"
        #return 'ATB International'
     
    elif key in ['U9ZBtbkGQ9kF5qyyDSw0']:
        
        return "9"
        #return 'Roaming Agent B2B' 

    else :
        
        return "0"
        #return 'Not Mapped'


# Define the function for the post transformation
def post_transformation(df1,msg):

    # Log the information for the debugging purpose
    print("I am from the post transformation function")
    print()

    # Try case
    try:
        # Read the data from the csv
        gd= df1

        # Log the shape
        print("Shape of the new dataframe ={}".format(gd.shape))
        print()

        # Log the columns
        print("Incoming normalized dataframe columns ={}".format(gd.columns))
        print()


        # Take the required colums only
        gd = gd[[
                    'uniqueid','city','step','searchtype','apiKey','remoteip','createdtime.hour',
                    'createdtime.dayOfWeek','createdtime.year','createdtime.monthValue','createdtime.second',
                    'createdtime.dayOfYear','createdtime.month','createdtime.dayOfMonth','createdtime.minute',
                    'createdtime.nano','checkoutdate','url','responsetime','cacheKey','totalStaticResultCount',
                    'status','checkindate','nights','dynamicresponsetime','checkin','staticresponsetime',
                    'requesttype','adults','totalDynamicResultCount','vendorId','rateplanid','time_stamp']
            ]

        # Print the before split dataframe shape
        print("Before split dataframe shape : {}".format(gd.shape))

        # Split the dataframe's on the step's i.e request and response
        gd_request= gd[gd['step'] =='request']
        #print("Request split dataframe shape : {}".format(gd_request.shape))

        # Take the required colums from the gd_request
        gd_request = gd_request[['uniqueid','city','searchtype','apiKey','remoteip','time_stamp']]
        #print("Response split dataframe shape : {}".format(gd_request.shape))

        # Split the required cols from the gd_response
        gd_response= gd[gd['step'] =='response']

        # Take only the required cols
        gd_response = gd_response[['uniqueid','checkoutdate','url','responsetime','cacheKey',
                                'totalStaticResultCount','status','checkindate','nights',
                                'dynamicresponsetime','checkin','staticresponsetime',
                                'requesttype','adults','totalDynamicResultCount','vendorId','rateplanid']]

        # Merge the dataframe based on the unique id 
        final_gd = pd.merge(gd_request, gd_response, on="uniqueid")

        # Apply the mapper function
        final_gd['Api_key_details'] = final_gd['apiKey'].apply(lambda x: mapper(x))

        # Apply the mapper function
        final_gd['searchtype'] = final_gd['searchtype'].apply(lambda x: type_mapper(x))

        
        # Log the information
        print("Final dataframe columns : {}".format(final_gd.columns))
        
        # Log the information
        print("Finally preprocessed .csv file shape is : {}".format(final_gd.shape))
        print()
        
        # Finally make the preprocessed csv file for the visualization
        final_gd.to_csv(os.path.join(dest_path,msg))

        # Print the file has been processed and downloaded
        print("All the process is completed , Check the generated file .csv file")
        print()
        print("*************************************Search Data ETL process end*************************************")
        print()

    # Handle the exception
    except Exception as e:

        # Log the information
        print("Some error occured in the post transformation")