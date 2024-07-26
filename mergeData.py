import pandas as pd
import glob
from getObj import directoryCheck

bangleStreamPath = 'connectClasses/bangleStream28/eskQd0UPDC' #Path to the folder with bangle stream data
annotationPath = 'connectClasses/userAnnotations/eskQd0UPDC' #Path to the folder with user annotations
annotataionTime = 300000 #Time to mark the annotation
calmData, stressedData = [], []

directoryCheck('mergedData')
directoryCheck('splitData')

bangleCSVs = glob.glob(bangleStreamPath + '/*.csv')
annotationCSVs = glob.glob(annotationPath + '/*.csv')
print("Bangle CSVs: ", bangleCSVs)
print("Annotation CSVs: ", annotationCSVs)

def associateBangleAnnotations(bangleData, annotationData, annotationTime):
    mergedData = bangleData.copy()
    mergedData.insert(len(mergedData.columns), 'annotations', [None] * len(bangleData))

    for _, annotRow in annotationData.iterrows():
        timestamp, annotation = annotRow['timeStamp'], annotRow['userAnnotation']
        lowerBound, upperBound = timestamp - annotationTime, timestamp + annotationTime

        for i, bangleRow in bangleData.iterrows():
            bufferMiddle = (bangleRow['bufferStart'] + bangleRow['bufferStop']) / 2
            if lowerBound <= bufferMiddle and bufferMiddle <= upperBound:
                mergedData.at[i, 'annotations'] = annotation

    return mergedData


def categorizeAnnotation(annotation):
    if annotation == "calm":
        return calmData
    else: 
        return stressedData

def splitData(associated, name):
    print("df shape: ", associated.shape)
    name = name.split('.')[0]
    nrows, _ = associated.shape
    col = associated.columns

    currentSeries = []
    i, previousRow = 1, associated.iloc[0]
    previousAnnotation = previousRow['annotations']
    num = 1
    while i < nrows:
        row = associated.iloc[i]
        annotation = row['annotations']

        if annotation == previousAnnotation:
            if annotation != None:
                # currently recording an annotation
                currentSeries.append(row)
        elif annotation == None:
            # stop an annotation record
            df = pd.DataFrame(columns=col, data=currentSeries)
            data = categorizeAnnotation(previousAnnotation)
            data.append(df)
            df.to_csv('splitData/' + name + str(num), index=False)
            currentSeries = []
            num += 1
        elif previousAnnotation == None:
            # start an annotation record
            currentSeries.append(row)
        else:
            # stop and start an annotation record
            df = pd.DataFrame(columns=col, data=currentSeries)
            data = categorizeAnnotation(previousAnnotation)
            data.append(df)
            currentSeries = [row]

        i += 1
        previousRow = row
        previousAnnotation = annotation

    if annotation != None:
        df = pd.DataFrame(columns=col, data=currentSeries)
        data = categorizeAnnotation(annotation)
        data.append(df)
    
    print(str(num) + " files created for " + name)

for bangleCSV in bangleCSVs:
    csvName = bangleCSV.split('/')[-1]
    for annotationCSV in annotationCSVs:
        annotationName = annotationCSV.split('/')[-1]
        if csvName == annotationName:
            print("Mergint data for: ", csvName)
            bangleData = pd.read_csv(bangleCSV)
            annotationsData = pd.read_csv(annotationPath + '/' + csvName)
            mergedData = associateBangleAnnotations(bangleData, annotationsData, annotataionTime)
            print(mergedData['annotations'].value_counts())
            mergedData.to_csv('mergedData/' + csvName, index=False)
            print("Splitting data for: ", csvName)
            splitData(mergedData, csvName)
        else:
            continue
    
