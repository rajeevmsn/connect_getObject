import requests
import pandas as pd
import os
import json
from io import StringIO
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
    if not os.path.exists(directory):
        os.makedirs(directory)

def bangleStream28(df):
    bufferStart = []
    bufferStop = []
    ax_M = []
    ay_M = []
    az_M = []
    cx_M = []
    cy_M = []
    cz_M = []
    cdx_M = []
    cdy_M = []
    cdz_M = []
    hrmRaw_M = []
    hrmFilt_M = []
    hrmBPM_M = []
    hrmConfidence_M = []
    ax_S = []
    ay_S = []
    az_S = []
    cx_S = []
    cy_S = []
    cz_S = []
    cdx_S = []
    cdy_S = []
    cdz_S = []
    hrmRaw_S = []
    hrmFilt_S = []
    hrmBPM_S = []
    hrmConfidence_S = []
    sessionId = []
    applicationId = []
    createdAt = []
    objectId = []


    df['bangle'] = df['bangle'].apply(lambda x: json.loads(x.replace("'", '"')) if isinstance(x, str) else x)
    for _, row in df.iterrows():
        session = row['sessionId']
        application = row['applicationId']
        created = row['createdAt']
        obj = row['objectId']
        for bangle in row['bangle']:
            bufferStart.append(bangle.get('bufferStart'))
            bufferStop.append(bangle.get('bufferStop'))
            ax_M.append(bangle.get('ax_M'))
            ay_M.append(bangle.get('ay_M'))
            az_M.append(bangle.get('az_M'))
            cx_M.append(bangle.get('cx_M'))
            cy_M.append(bangle.get('cy_M'))
            cz_M.append(bangle.get('cz_M'))
            cdx_M.append(bangle.get('cdx_M'))
            cdy_M.append(bangle.get('cdy_M'))
            cdz_M.append(bangle.get('cdz_M'))
            hrmRaw_M.append(bangle.get('hrmRaw_M'))
            hrmFilt_M.append(bangle.get('hrmFilt_M'))
            hrmBPM_M.append(bangle.get('hrmBPM_M'))
            hrmConfidence_M.append(bangle.get('hrmConfidence_M'))
            ax_S.append(bangle.get('ax_S'))
            ay_S.append(bangle.get('ay_S'))
            az_S.append(bangle.get('az_S'))
            cx_S.append(bangle.get('cx_S'))
            cy_S.append(bangle.get('cy_S'))
            cz_S.append(bangle.get('cz_S'))
            cdx_S.append(bangle.get('cdx_S'))
            cdy_S.append(bangle.get('cdy_S'))
            cdz_S.append(bangle.get('cdz_S'))
            hrmRaw_S.append(bangle.get('hrmRaw_S'))
            hrmFilt_S.append(bangle.get('hrmFilt_S'))
            hrmBPM_S.append(bangle.get('hrmBPM_S'))
            hrmConfidence_S.append(bangle.get('hrmConfidence_S'))
            sessionId.append(session)
            applicationId.append(application)
            createdAt.append(created)
            objectId.append(obj)
    bangleDf = pd.DataFrame({
        'sessionId': sessionId,
        'applicationId': applicationId,
        'createdAt': createdAt,
        'objectId': objectId,
        'bufferStart': bufferStart,
        'bufferStop': bufferStop,
        'ax_M': ax_M,
        'ay_M': ay_M,
        'az_M': az_M,
        'cx_M': cx_M,
        'cy_M': cy_M,
        'cz_M': cz_M,
        'cdx_M': cdx_M,
        'cdy_M': cdy_M,
        'cdz_M': cdz_M,
        'hrmRaw_M': hrmRaw_M,
        'hrmFilt_M': hrmFilt_M,
        'hrmBPM_M': hrmBPM_M,
        'hrmConfidence_M': hrmConfidence_M,
        'ax_S': ax_S,
        'ay_S': ay_S,
        'az_S': az_S,
        'cx_S': cx_S,
        'cy_S': cy_S,
        'cz_S': cz_S,
        'cdx_S': cdx_S,
        'cdy_S': cdy_S,
        'cdz_S': cdz_S,
        'hrmRaw_S': hrmRaw_S,
        'hrmFilt_S': hrmFilt_S,
        'hrmBPM_S': hrmBPM_S,
        'hrmConfidence_S': hrmConfidence_S
    })
    print(bangleDf.shape)
    return bangleDf

def userAnnotations(df):
    df['events'] = df['events'].apply(lambda x: json.loads(x.replace("'", '"'))if isinstance(x, str) else x)
    eventsData = []
    for index, row in df.iterrows():
        for event in row['events']:
            eventEntry = {
                'sessionId': row['sessionId'],
                'userId': row['userId'],
                'applicationId': row['applicationId'],
                'createdAt': row['createdAt'],
                'updatedAt': row['updatedAt'],
                'objectId': row['objectId'],
                'timeStamp': event['timeStamp'],
                'userAnnotation': event['userAnnotation'],
                'confidence': event.get('confidence', None),
                'activity': event.get('activity', None)
            }
            eventsData.append(eventEntry)
    
    eventsDf = pd.DataFrame(eventsData)
    print(eventsDf.shape)
    return eventsDf

functionMapping = {
    'bangleStream28': bangleStream28,
    'userAnnotations': userAnnotations
}

def saveData(df, dataClassFolder, functionMapping):
    print(f"Number of different userId: {len(df['userId'].unique())}")
    userIdCount = df['userId'].value_counts()
    print(userIdCount)
    df['applicationId'] = df['applicationId'].fillna('Tests12345')
    for applicationId in df['applicationId'].unique():
        dfApp = df[df['applicationId'] == applicationId]
        print(f"Application ID: {applicationId}")
        folderPath = os.path.join(dataClassFolder, applicationId)
        directoryCheck(folderPath)

        functionName = dataClassFolder.split('/')[-1]
        if functionName in functionMapping:
            function = functionMapping[functionName]
        else:
            raise ValueError(f"Function {functionName} not found in function mapping.")

        # Save the individual dataframes as CSV files in the folder
        df['userId'] = df['userId'].fillna('users12345')
        for userId in dfApp['userId'].unique():
            dfUser = pd.DataFrame(dfApp[dfApp['userId'] == userId])
            dfFinal = function(dfUser)
            filePath = os.path.join(folderPath, f"{userId}.{format}")
            dfFinal.to_csv(filePath, index=False)
            print(f"File saved: {filePath}")


def main():
    directoryCheck(connectClasses)
    for dataClassItem in dataClass:
        dataClassFolder = os.path.join(connectClasses, dataClassItem)
        directoryCheck(dataClassFolder)
        data = retrieveAllData(dataClassItem, sessionToken)
        df = pd.DataFrame(data)
        saveData(df, dataClassFolder, functionMapping)

if __name__ == '__main__':
    main()
