import requests
import pandas as pd
import os
from dotenv import load_dotenv

connectURL = 'https://connect-project.io'
parseApplication = 'connect'

#folder name to save the data
connectClasses = "connectClasses"

load_dotenv(".env")
sessionToken = os.getenv("sessionToken")
dataClass = os.getenv("dataClass").split(",")
format = os.getenv("format")

print(f"sessionToken: {sessionToken}")
print(f"dataClass: {dataClass}")

def retrieveData(connectClass, connectToken, skip=0):
    """Retrieve data from the specified connect class."""
    print(f"Retrieving {connectClass} data")
    url = f'{connectURL}/parse/classes/{connectClass}'
    headers = {
        'x-parse-application-id': parseApplication,
        'x-parse-session-token': connectToken,
    }
    params = {
        'limit': '100',
        'skip': str(skip),
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    data = response.json()['results']
    return data

def retrieveAllData(connectClass, connectToken):
    """Retrieve all data from the specified connect class."""
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

def directoryCheck(directory):
    """Check if the directory exists, if not create it."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def saveData(df, dataClassFolder):
    print(f"Number of different userId: {len(df['userId'].unique())}")
    userIdCount = df['userId'].value_counts()
    print(userIdCount)
    df['applicationId'] = df['applicationId'].fillna('Tests12345')
    for applicationId in df['applicationId'].unique():
        dfApp = df[df['applicationId'] == applicationId]
        print(f"Application ID: {applicationId}")
        folderPath = os.path.join(dataClassFolder, applicationId)
        directoryCheck(folderPath)
        # Save the individual dataframes as CSV files in the folder
        df['userId'] = df['userId'].fillna('users12345')
        for userId in dfApp['userId'].unique():
            dfUser = dfApp[dfApp['userId'] == userId]
            file_path = os.path.join(folderPath, f"{userId}.{format}")
            dfUser.to_csv(file_path, index=False)
            print(f"File saved: {file_path}")


def main():
    directoryCheck(connectClasses)
    for dataClassItem in dataClass:
        dataClassFolder = os.path.join(connectClasses, dataClassItem)
        directoryCheck(dataClassFolder)

        data = retrieveAllData(dataClassItem, sessionToken)
        print(len(data))

        df = pd.DataFrame(data)
        print(df.head(2))
        print(df.columns)

        saveData(df, dataClassFolder)

if __name__ == '__main__':
    main()