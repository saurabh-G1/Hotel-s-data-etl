# Import the required methods
from  extract import extract_data

# Code flow as defined in steps :

# [Step 1]
# a. Calling the extract data from the extract.py file 
# b. It will bring the data from the elastic search server to this one and convert it into the dataframe
# c. After converting into the dataframe it will call the transform.py

# [Step 2] 
# a. Flatten the json using json normalize 
# b. Calling the post_transformation method

# [Step 3]
# a. Split the dataframe based on the step i.e request / response
# b. Merge the 2 splitted dataframe using the unqiue id
# c. At last apply the mapper function for api key's explanation

# Now calling the main method
if __name__ == '__main__':
    extract_data()


