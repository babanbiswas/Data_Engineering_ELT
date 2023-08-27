import requests
import snowflake.connector

# Make an API request and get the JSON response
api_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
response = requests.get(api_url)
print(response.status_code)
api_data = response.json()
print(api_data)

# Assuming data is a list of dictionaries
# You can iterate through it to extract relevant fields
# and perform any necessary transformations

#Step 3 : Load Data into Snowflake Table



# Snowflake connection parameters
snowflake_config = {
    "user": "bob123",
    "password": "Bob123*#",
    "account": "am33284.central-india.azure",
    "warehouse": "COMPUTE_WH",
    "database": "FIRST_ELT_PR",
    "schema": "FIRST_ELT_SC"
}

# Connect to Snowflake
conn = snowflake.connector.connect(**snowflake_config)
#print(conn)
cur = conn.cursor()

transformed_data = []

for record in api_data['data']:
    transformed_data.append(record)
    #print(record)

# Load data from the extracted JSON into the staging table
# You need to construct the SQL COPY command based on your data and staging table structure

try:
    for row_dict in transformed_data:
        placeholders = ', '.join(['%s']*len(row_dict))
        print(placeholders)
        values = tuple(row_dict.values())
        print(values)
        insert_query = f"INSERT INTO FIRST_ELT_PR.FIRST_ELT_SC.US_POPULATION (ID_Nation, NATION, ID_Year, YEAR, POPULATION, Slug_Nation) VALUES ({placeholders})"
        print(insert_query)
        cur.execute(insert_query, values)

finally:
    cur.close()
