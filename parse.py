logging = False

from pickle import load
from googleapiclient.discovery import build
from io import FileIO
from googleapiclient.http import MediaIoBaseDownload
from urllib.request import urlopen
from urllib.error import HTTPError
from codecs import iterdecode
from csv import reader
from os import listdir, remove
from time import sleep
from datetime import datetime

with open('token.pickle', 'rb') as token:
    creds = load(token)
service = build('sheets', 'v4', credentials=creds)

with open('api.key', 'r') as key:
    apiKey = key.read()
drive = build('drive', 'v3', developerKey=apiKey)

fieldMap = {
    'People Immunized with One Dose'                                     : 'First Dose',
    'People Immunized with Two Doses'                                    : 'Second Dose',
    'Confirmed COVID-19'                                                 : 'Confirmed',
    'COVID-19 Persons Under Investigation'                               : 'Under Investigation',
    'Cumulative COVID-19 Cases in Colorado by Date of Illness Onset'     : 'Cases by Onset',
    'Cumulative COVID-19 Cases in Colorado by Date Reported to the State': 'Cases',
    'Cumulative Deaths Among COVID-19 Cases in Colorado by Date of Death': 'Deaths',
    'Cumulative People Tested at Lab'                                    : 'Tests',
    'Cases of COVID-19 in Colorado by County'                            : 'Cases',
    'Deaths Among COVID-19 Cases in Colorado by County'                  : 'Deaths',
    'Total COVID-19 Tests Performed in Colorado by County'               : 'Tests',
}
stateFields = list(fieldMap)[:8]
countyFields = list(fieldMap)[8:]

headers = [
    'Date'                    ,
    'First Dose'              , '', '%       ',
    'Second Dose'             , '', '%       ',
    'Confirmed Cases'         ,
    'Under Investigation'     ,
    'by date of illness onset', '       ',
    'by date reported'        , '',
    'by date of death'        , '',
    'Tests'                   , '       ',
    'Positive %'              , '',
]

counties = {
    'Denver'    : {},
    'El Paso'   : {},
    'Arapahoe'  : {},
    'Jefferson' : {},
    'Adams'     : {},
    'Larimer'   : {},
    'Douglas'   : {},
    'Boulder'   : {},
    'Weld'      : {},
    'Pueblo'    : {},
    'Mesa'      : {},
    'Broomfield': {},
    'Other'     : {},
}

def extendHeaders(titles):
    headers.extend(titles)
    for county in counties:
        headers.append(county)

extendHeaders(['Cases'            , 'Colorado'])
extendHeaders(['Weekly Cases'     , 'Colorado'])
extendHeaders(['Deaths'           , 'Colorado'])
extendHeaders(['Weekly Deaths'    , 'Colorado'])
extendHeaders(['Tests'            , 'Colorado'])
extendHeaders(['Weekly Tests'     , 'Colorado'])
extendHeaders(['Positive %'       , 'Colorado'])
extendHeaders(['Weekly Positive %', 'Colorado'])

def printNow(*message):
    print(*message, flush=True)

firstRun = True
failedLastRun = False
lastRun = ''
lastDate = ''

while True:
    if not firstRun:
        sleep(3)
    firstRun = False

    thisRun = str(datetime.now())[:15]
    if thisRun == lastRun:
        continue
    lastRun = thisRun

    data = {
        'Colorado': {
            'First Dose'         : {},
            'Second Dose'        : {},
            'Confirmed'          : {},
            'Under Investigation': {},
            'Cases by Onset'     : {},
            'Cases'              : {},
            'Deaths'             : {},
            'Tests'              : {},
        }
    }

    for county in counties:
        data[county] = {
            'Cases' : {},
            'Deaths': {},
            'Tests' : {},
        }

    def formatDate(date):
        return date[6:10] + '-' + date[0:2] + '-' + date[3:5]

    # CDPHE COVID19 State-Level Expanded Case Data
    # https://data-cdphe.opendata.arcgis.com/datasets/15883575464d46f686044d2c1aa84ef9_0
    if logging:
        printNow('Getting    state    data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/15883575464d46f686044d2c1aa84ef9_0.csv')
    except HTTPError as e:
        printNow(str(datetime.now())[:19], '-- Error getting state data:', e.code)
        failedLastRun = True
        continue
    stateData = reader(iterdecode(response, 'utf-8-sig'))
    if logging:
        printNow('Processing state    data')

    readFields = True
    for row in stateData:
        if readFields:
            idescription = row.index('description')
            idate        = row.index('date')
            ivalue       = row.index('value')
            readFields = False

        description = row[idescription]
        date        = row[idate]
        value       = row[ivalue]

        if description in stateFields:
            data['Colorado'][fieldMap[description]][formatDate(date)] = int(value)

    dates = sorted(list(set(data['Colorado']['Cases by Onset']) | set(data['Colorado']['Cases'])))
    if dates[-1] == lastDate and not failedLastRun:
        continue
    lastDate = dates[-1]

    # https://drive.google.com/drive/folders/1bjQ7LnhU8pBR3Ly63341bCULHFqc7pMw
    if logging:
        printNow('Getting    hospital data')
    try:
        hospitalFileId = drive.files().list(q="'1bjQ7LnhU8pBR3Ly63341bCULHFqc7pMw' in parents", fields="files(id,name)", orderBy="name", pageSize=1000).execute()['files'][-1]['id']
        fh = FileIO('hospitalData.csv', 'w')
        downloader = MediaIoBaseDownload(fh, drive.files().get_media(fileId=hospitalFileId))
        while not downloader.next_chunk():
            pass
        fh.close()
    except HTTPError as e:
        printNow(str(datetime.now())[:19], '-- Error getting hospital data:', e.code)
        failedLastRun = True
        continue

    with open('hospitalData.csv') as file:
        hospitalData = reader(file)
        if logging:
            printNow('Processing hospital data')

        readFields = True
        for row in hospitalData:
            if readFields:
                icategory    = row.index('category')
                idescription = row.index('description')
                iregion      = row.index('region')
                idate        = row.index('date')
                imetric      = row.index('metric')
                ivalue       = row.index('value')
                readFields = False

            category    = row[icategory]
            description = row[idescription]
            region      = row[iregion]
            date        = row[idate]
            metric      = row[imetric]
            value       = row[ivalue]

            if category == 'Hospital Level' and description == 'Currently Hospitalized' and region == 'Colorado' and metric in stateFields:
                data['Colorado'][fieldMap[metric]][date] = int(value)
    remove('hospitalData.csv')

    # CDPHE COVID19 Vaccine Daily Summary Statistics
    # https://data-cdphe.opendata.arcgis.com/datasets/a681d9e9f61144b2977badb89149198c_0
    if logging:
        printNow('Getting    vaccine  data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/a681d9e9f61144b2977badb89149198c_0.csv')
    except HTTPError as e:
        printNow(str(datetime.now())[:19], '-- Error getting vaccine data:', e.code)
        failedLastRun = True
        continue
    vaccineData = reader(iterdecode(response, 'utf-8-sig'))
    if logging:
        printNow('Processing vaccine  data')

    readFields = True
    for row in vaccineData:
        if readFields:
            isection      = row.index('section')
            icategory     = row.index('category')
            imetric       = row.index('metric')
            ivalue        = row.index('value')
            ipublish_date = row.index('publish_date')
            readFields = False

        section      = row[isection]
        category     = row[icategory]
        metric       = row[imetric]
        value        = row[ivalue]
        publish_date = row[ipublish_date]

        if section == 'State Data' and category == 'Cumulative counts to date' and metric in stateFields:
            data['Colorado'][fieldMap[metric]][formatDate(publish_date)] = int(value)


    # COVID19 Positivity Data from Clinical Laboratories
    # https://data-cdphe.opendata.arcgis.com/datasets/667a028c66e64be79d1f801cd6e6f304_0
    if logging:
        printNow('Getting    testing  data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/667a028c66e64be79d1f801cd6e6f304_0.csv')
    except HTTPError as e:
        printNow(str(datetime.now())[:19], '-- Error getting testing data:', e.code)
        failedLastRun = True
        continue
    testingData = reader(iterdecode(response, 'utf-8-sig'))
    if logging:
        printNow('Processing testing  data')

    field = 'Cumulative People Tested at Lab'
    readFields = True
    for row in testingData:
        if readFields:
            iDesc_     = row.index('Desc_')
            iAttr_Date = row.index('Attr_Date')
            iMetric    = row.index('Metric')
            iValue     = row.index('Value')
            readFields = False

        Desc_     = row[iDesc_]
        Attr_Date = row[iAttr_Date]
        Metric    = row[iMetric]
        Value     = row[iValue]

        if Desc_ == 'Daily COVID-19 PCR Test Data From Clinical Laboratories' and Metric in ['Cumulative People Tested at CDPHE State Lab', 'Cumulative People Tested at Non-CDPHE (Commerical) Labs']:
            date = formatDate(Attr_Date)
            if date not in data['Colorado'][fieldMap[field]]:
                data['Colorado'][fieldMap[field]][date] = 0
            data['Colorado'][fieldMap[field]][date] += int(Value)

    # CDPHE COVID19 County-Level Open Data Repository
    # https://data-cdphe.opendata.arcgis.com/datasets/cdphe-covid19-county-level-open-data-repository
    if logging:
        printNow('Getting    county   data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/8ff1603466cb4fadaff7018612dc58a0_0.csv')
    except HTTPError as e:
        printNow(str(datetime.now())[:19], '-- Error getting county data:', e.code)
        failedLastRun = True
        continue
    countyData = reader(iterdecode(response, 'utf-8-sig'))
    if logging:
        printNow('Processing county   data')

    readFields = True
    for row in countyData:
        if readFields:
            iLABEL  = row.index('LABEL')
            iDesc_  = row.index('Desc_')
            iMetric = row.index('Metric')
            iValue  = row.index('Value')
            iDate   = row.index('Date')
            readFields = False

        LABEL  = row[iLABEL]
        Desc_  = row[iDesc_]
        Metric = row[iMetric]
        Value  = row[iValue]
        Date   = row[iDate]

        if LABEL not in counties and LABEL not in ['Note', 'Unknown Or Pending County', 'Out Of State County', 'International']:
            LABEL = 'Other'
        if Desc_ == 'Cases of COVID-19 in Colorado by County' and Metric == 'Deaths': # errors in the data
            Desc_ = 'Deaths Among COVID-19 Cases in Colorado by County'
        date = formatDate(Date)
        if LABEL in counties and Desc_ in countyFields and Metric not in ['Percent of tests by PCR', 'Percent of tests by Serology']:
            if date not in data[LABEL][fieldMap[Desc_]]:
                data[LABEL][fieldMap[Desc_]][date] = 0
            data[LABEL][fieldMap[Desc_]][date] += int(Value)

    tsvData = '\t'.join(headers) + '\n'
    sheetData = [headers]

    def daily(region, field, i):
        if dates[i] not in data[region][field]:
            return ''

        today = data[region][field][dates[i]]
        if i > 0 and dates[i-1] in data[region][field]:
            yesterday = data[region][field][dates[i-1]]
        else:
            yesterday = 0
        return max(0, today - yesterday)

    def weekly(region, field, i):
        if dates[i] not in data[region][field]:
            return ''

        today = data[region][field][dates[i]]
        if i > 6 and dates[i-7] in data[region][field]:
            lastweek = data[region][field][dates[i-7]]
        else:
            lastweek = 0
        return max(0, today - lastweek)

    def strRound(num):
        if num == '':
            return ''

        return str(round(num/7, 3))

    if logging:
        printNow('Building output')

    for i in range(len(dates)):
        date = dates[i]

        if i>0:
            for field in stateFields:
                if date not in data['Colorado'][fieldMap[field]] and dates[i-1] in data['Colorado'][fieldMap[field]]:
                    data['Colorado'][fieldMap[field]][date] = 0
            for field in countyFields:
                for county in counties:
                    if date not in data[county][fieldMap[field]] and dates[i-1] in data[county][fieldMap[field]]:
                        data[county][fieldMap[field]][date] = 0

        if i < dates.index('2020-03-01'):
            continue

        row = date + '\t'
        sheetRow = [date]

        for field in ['First Dose', 'Second Dose']:
            row += str(daily('Colorado', field, i)) + '\t' + strRound(weekly('Colorado', field, i)) + '\t'
            sheetRow.extend([str(daily('Colorado', field, i)), strRound(weekly('Colorado', field, i))])

            if date in data['Colorado'][field]:
                row += str(round(100 * data['Colorado'][field][date] / 5763976, 3))
                sheetRow.append(str(round(100 * data['Colorado'][field][date] / 5763976, 3)))
            else:
                sheetRow.append('')
            row += '\t'

        if date in data['Colorado']['Confirmed']:
            row += str(data['Colorado']['Confirmed'][date])
            sheetRow.append(str(data['Colorado']['Confirmed'][date]))
        else:
            sheetRow.append('')
        row += '\t'
        if date in data['Colorado']['Under Investigation']:
            row += str(data['Colorado']['Under Investigation'][date])
            sheetRow.append(str(data['Colorado']['Under Investigation'][date]))
        else:
            sheetRow.append('')
        row += '\t'

        for field in ['Cases by Onset', 'Cases', 'Deaths', 'Tests']:
            row += str(daily('Colorado', field, i)) + '\t' + strRound(weekly('Colorado', field, i)) + '\t'
            sheetRow.extend([str(daily('Colorado', field, i)), strRound(weekly('Colorado', field, i))])

        if daily ('Colorado', 'Tests', i) != '' and daily('Colorado', 'Tests', i) > 0:
            if daily('Colorado', 'Cases', i) != '':
                row += str(round(100 * daily('Colorado', 'Cases', i) / daily('Colorado', 'Tests', i) , 3))
                sheetRow.append(str(round(100 * daily('Colorado', 'Cases', i) / daily('Colorado', 'Tests', i) , 3)))
            else:
                row += '0'
                sheetRow.append('0')
        else:
            sheetRow.append('')
        row += '\t'
        if weekly('Colorado', 'Tests', i) != '' and weekly('Colorado', 'Tests', i) > 0:
            if weekly('Colorado', 'Cases', i) != '':
                row += str(round(100 * weekly('Colorado', 'Cases', i) / weekly('Colorado', 'Tests', i), 3))
                sheetRow.append(str(round(100 * weekly('Colorado', 'Cases', i) / weekly('Colorado', 'Tests', i), 3)))
            else:
                row += '0'
                sheetRow.append('0')
        else:
            sheetRow.append('')
        row += '\t'

        for field in ['Cases', 'Deaths', 'Tests']:
            row += '\t'
            sheetRow.append('')
            for region in ['Colorado'] + list(counties):
                row += str(daily(region, field, i)) + '\t'
                sheetRow.append(str(daily(region, field, i)))
            row += '\t'
            sheetRow.append('')
            for region in ['Colorado'] + list(counties):
                row += strRound(weekly(region, field, i)) + '\t'
                sheetRow.append(strRound(weekly(region, field, i)))

        row += '\t'
        sheetRow.append('')
        for region in ['Colorado'] + list(counties):
            if daily(region, 'Tests', i) != '' and daily(region, 'Tests', i) > 0:
                if daily(region, 'Cases', i) != '':
                    row += str(round(100 * daily(region, 'Cases', i) / daily(region, 'Tests', i) , 3))
                    sheetRow.append(str(round(100 * daily(region, 'Cases', i) / daily(region, 'Tests', i) , 3)))
                else:
                    row += '0'
                    sheetRow.append('0')
            else:
                sheetRow.append('')
            row += '\t'
        row += '\t'
        sheetRow.append('')
        for region in ['Colorado'] + list(counties):
            if weekly(region, 'Tests', i) != '' and weekly(region, 'Tests', i) > 0:
                if weekly(region, 'Cases', i) != '':
                    row += str(round(100 * weekly(region, 'Cases', i) / weekly(region, 'Tests', i), 3))
                    sheetRow.append(str(round(100 * weekly(region, 'Cases', i) / weekly(region, 'Tests', i), 3)))
                else:
                    row += '0'
                    sheetRow.append('0')
            else:
                sheetRow.append('')
            row += '\t'

        tsvData += row[:-1] + '\n'
        sheetData.append(sheetRow)

    with open('data.tsv', 'w') as newFile:
        newFile.write(tsvData)

    service.spreadsheets().values().update(
        spreadsheetId = '1dfP3WLeU9T2InpIzNyo65R8d_e7NpPea9zKaldEdYRA',
        valueInputOption = 'USER_ENTERED',
        range = 'Data!A1:EI',
        body = dict(
            majorDimension = 'ROWS',
            values = sheetData
        )
    ).execute()

    printNow(str(datetime.now())[:19], '-- Data is current through', dates[-1])
    failedLastRun = False
    logging = False
