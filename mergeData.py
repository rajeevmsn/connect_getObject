import pandas as pd
import glob
from getObj import directoryCheck

bangleStreamPath = 'connectClasses/bangleStream28/cnhb88Civu'
annotationPath = 'connectClasses/userAnnotations/cnhb88Civu'
annotataionTime = 100000 #Time to mark the annotation

directoryCheck('mergedData')

bangleCSVs = glob.glob(bangleStreamPath + '/*.csv')
annotationCSVs = glob.glob(annotationPath + '/*.csv')

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

for bangleCSV in bangleCSVs:
    csvName = bangleCSV.split('/')[-1]
    print("Mergint data for: ", csvName)
    bangleData = pd.read_csv(bangleCSV)
    annotationsData = pd.read_csv(annotationPath + '/' + csvName)
    mergedData = associateBangleAnnotations(bangleData, annotationsData, annotataionTime)
    print(mergedData['annotations'].value_counts())
    mergedData.to_csv('mergedData/' + csvName, index=False)

