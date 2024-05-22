import requests
import pandas as pd
import os
from dotenv import load_dotenv

connectURL = 'https://connect-project.io'
parseApplication = 'connect'

# Load environment variables from .env file
load_dotenv(".env")

# Get the sessionToken and Dataclass from environment variables
sessionToken = os.getenv("sessionToken")
dataClass = os.getenv("dataClass")

print(f"sessionToken: {sessionToken}")
print(f"dataClass: {dataClass}")

def retrieveData(connectClass, connectToken, skip=0):
    #Parameters for requests
    url = f'{connectURL}/parse/classes/{connectClass}'
    headers = {
        'x-parse-application-id': parseApplication,
        'x-parse-session-token': connectToken,
    }

    params = {
        'limit': '100',
        'skip': str(skip),
    }

    # Retrieve data
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    data = response.json()['results']

    return data

def retrieveAllData(connectClass, connectToken):
    print(f"{'-'*10}Retrieving {connectClass} {'-'*10}")
    count=0
    allData = []

    while True:
        tempData = retrieveData(connectClass, connectToken, skip=count)
        if tempData != []:
            allData += tempData
            count += len(tempData)
            print(f"{len(tempData)} records retrieved; total: {count} records")
        else:
            print(f'Retrieval complete! total: {count} records')
            break
    
    return allData

data = retrieveAllData(dataClass, sessionToken)
print(len(data))

df = pd.DataFrame(data)
print(df.head(2))
print(df.columns)

print(f"Number of different userId: {len(df['userId'].unique())}")
#print(df['userId'].tolist())
userIdCount = df['userId'].value_counts()
print(userIdCount)
# Create the connectClasses folder
connectClasses = "connectClasses"
if not os.path.exists(connectClasses):
    os.makedirs(connectClasses)

# Create the dataClass folder inside connectClasses
dataClassFolder = os.path.join(connectClasses, dataClass)
if not os.path.exists(dataClassFolder):
    os.makedirs(dataClassFolder)
    # Split data based on applicationId and save in folder with applicationId name
    df['applicationId'].fillna('Tests12345', inplace=True)
    for applicationId in df['applicationId'].unique():
        dfApp = df[df['applicationId'] == applicationId]
        print(f"Application ID: {applicationId}")
        folderPath = os.path.join(dataClassFolder, applicationId)
        if not os.path.exists(folderPath):
            os.makedirs(folderPath)
        # Save the individual dataframes as CSV files in the folder
        df['userId'].fillna('users12345', inplace=True)
        for userId in dfApp['userId'].unique():
            dfUser = dfApp[dfApp['userId'] == userId]
            file_path = os.path.join(folderPath, f"{userId}.csv")
            dfUser.to_csv(file_path, index=False)
