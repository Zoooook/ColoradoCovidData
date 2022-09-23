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
import datetime
from math import floor

with open('token.pickle', 'rb') as token:
    creds = load(token)
service = build('sheets', 'v4', credentials=creds)

with open('api.key', 'r') as key:
    apiKey = key.read()
drive = build('drive', 'v3', developerKey=apiKey)

fieldMap = {
    'People Immunized with One Dose'                                     : 'First Dose',
    'People Fully Immunized'                                             : 'All Doses',
    'People with Additional Doses'                                       : 'Additional Doses',
    'Confirmed COVID-19'                                                 : 'Confirmed',
    'COVID-19 Persons Under Investigation'                               : 'Under Investigation',
    'Cumulative COVID-19 Cases in Colorado by Date of Illness Onset'     : 'Cases by Onset',
    'Cumulative COVID-19 Cases in Colorado by Date Reported to the State': 'Cases',
    'Cumulative Deaths Among COVID-19 Cases in Colorado by Date of Death': 'Deaths',
    'Cumulative People Tested at Lab'                                    : 'Tests',

    'Cases of COVID-19 in Colorado by County'                            : 'Cases',
    'Deaths Among COVID-19 Cases in Colorado by County'                  : 'Deaths',
    'Total COVID-19 Tests Performed in Colorado by County'               : 'Tests',

    'Other'                                                              : 'Other',
    'B.1.1.7 Alpha'                                                      : 'B.1.1.7',
    'B.1.351 Beta'                                                       : 'B.1.351',
    'P.1 Gamma'                                                          : 'P.1',
    'B.1.617.2 Delta'                                                    : 'B.1.617.2',
    'AY.2 Delta'                                                         : 'AY.2',
    'AY.1 Delta'                                                         : 'AY.1',
    'B.1.429'                                                            : 'B.1.429',
    'B.1.427'                                                            : 'B.1.427',
    'B.1.1.529 Omicron (BA.1)'                                           : 'BA.1',
    'B.1.1.529 Omicron (BA.2)'                                           : 'BA.2',
    'BA.2.12.1 Omicron'                                                  : 'BA.2.12.1',
    'BA.4 Omicron'                                                       : 'BA.4',
    'BA.5 Omicron'                                                       : 'BA.5',
    'BA.4.6 Omicron'                                                     : 'BA.4.6',
    'BA.2.75 Omicron'                                                    : 'BA.2.75',
    'BF.7 Omicron'                                                       : 'BF.7',
}
stateFields = list(fieldMap)[:9]
countyFields = list(fieldMap)[9:12]
variantFields = list(fieldMap)[12:]

headers = [
    'Last Updated'            ,
    'Date'                    , '',
    'First Dose'              , '%       ', '',
    'All Doses'               , '%       ', '',
    'Additional Doses'        , '%       ',
    'Date'                    ,
    'Confirmed Cases'         ,
    'Under Investigation'     , '',
    'by date of illness onset', '       ',
    'by date reported'        , '',
    'by date of death'        , '',
    'Tests'                   , '       ',
    'Positive %'              ,
]

counties = [
    'Denver',
    'El Paso',
    'Arapahoe',
    'Jefferson',
    'Adams',
    'Larimer',
    'Douglas',
    'Boulder',
    'Weld',
    'Pueblo',
    'Mesa',
    'Broomfield',
    'Other',
]

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

variantHeaders = [fieldMap[field] for field in variantFields]

def printNow(*message):
    print(*message, flush=True)

firstRun         = True
lastRun          = ''
lastVaccineDate  = ''
lastHospitalDate = ''
lastStateDate    = ''
lastTestDate     = ''
lastCountyDate   = ''
lastVariantDate  = ''
lastVariantData  = []
lastUpdated      = ['', '', '', '', '', '']

while True:
    if not firstRun:
        sleep(3)
    firstRun = False

    thisRun = str(datetime.datetime.now())[:15]
    if thisRun == lastRun:
        continue
    lastRun = thisRun

    now = str(datetime.datetime.now())[:16]

    if now[-5:] == '00:00':
        printNow('')

    for i in range(len(lastUpdated)):
        if not lastUpdated[i]:
            lastUpdated[i] = now

    data = {
        'Colorado': {
            'First Dose'         : {},
            'All Doses'          : {},
            'Additional Doses'   : {},
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

    for variant in variantHeaders:
        data['Colorado'][variant] = {}

    def formatDate(date):
        return date[6:10] + '-' + date[0:2] + '-' + date[3:5]

    def saturday(date):
        return str(datetime.date(int(date[6:10]), int(date[0:2]), int(date[3:5])) + datetime.timedelta(days=6))



    # CDPHE COVID19 Vaccine Daily Summary Statistics
    # https://data-cdphe.opendata.arcgis.com/datasets/fa9730c29ee24c7b8b52361ae3e5ca53_0
    if logging:
        printNow('Getting    vaccine  data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/fa9730c29ee24c7b8b52361ae3e5ca53_0.csv')
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
                continue

            section      = row[isection]
            category     = row[icategory]
            metric       = row[imetric]
            value        = row[ivalue]
            publish_date = row[ipublish_date]

            if section == 'State Data' and category == 'Cumulative counts to date' and metric in stateFields:
                if publish_date == '08/04/2021' and value in ['3379501','3099180']:
                    continue
                data['Colorado'][fieldMap[metric]][formatDate(publish_date)] = int(value)
    except HTTPError as e:
        printNow(now, '-- Error getting vaccine data:', e.code)
        continue
    except Exception as e:
        printNow(now, '-- Error getting vaccine data:', str(e))
        continue

    vaccineDates = sorted(list(set(data['Colorado']['First Dose']) | set(data['Colorado']['All Doses'])))
    if vaccineDates[-1] != lastVaccineDate:
        updateData = True
        lastVaccineDate = vaccineDates[-1]
        lastUpdated[0] = now
        printNow('Vaccine  data updated to', lastVaccineDate[5:])

    def parseHospitalData(filename):
        with open(filename) as file:
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
                    continue

                category    = row[icategory]
                description = row[idescription]
                region      = row[iregion]
                date        = row[idate]
                metric      = row[imetric]
                value       = row[ivalue]

                if category == 'Hospital Level' and description == 'Currently Hospitalized' and region == 'Colorado' and metric in stateFields:
                    data['Colorado'][fieldMap[metric]][date] = int(value)

    # https://drive.google.com/drive/folders/1bjQ7LnhU8pBR3Ly63341bCULHFqc7pMw
    def getHospitalData():
        try:
            hospitalFileId = drive.files().list(q="'1bjQ7LnhU8pBR3Ly63341bCULHFqc7pMw' in parents", fields="files(id,name)", orderBy="name", pageSize=1000).execute()['files'][-1]['id']
            fh = FileIO('hospitalData.csv', 'w')
            downloader = MediaIoBaseDownload(fh, drive.files().get_media(fileId=hospitalFileId))
            while not downloader.next_chunk():
                pass
            fh.close()
            parseHospitalData('hospitalData.csv')
            remove('hospitalData.csv')
            parseHospitalData('covid19_hospital_data_2022-03-15.csv')
            return True
        except HTTPError as e:
            printNow(now, '-- Error getting hospital data:', e.code)
            return False
        except Exception as e:
            if str(e) == '[WinError 10053] An established connection was aborted by the software in your host machine':
                sleep(1)
                return getHospitalData()
            else:
                printNow(now, '-- Error getting hospital data:', str(e))
                return False

    if logging:
        printNow('Getting    hospital data')
    if not getHospitalData():
        continue

    hospitalDates = sorted(list(set(data['Colorado']['Confirmed']) | set(data['Colorado']['Under Investigation'])))
    if hospitalDates[-1] != lastHospitalDate:
        updateData = True
        lastHospitalDate = hospitalDates[-1]
        lastUpdated[1] = now
        printNow('Hospital data updated to', lastHospitalDate[5:])

    # CDPHE COVID19 State-Level Expanded Case Data
    # https://data-cdphe.opendata.arcgis.com/datasets/cdphe-covid19-state-level-expanded-case-data
    if logging:
        printNow('Getting    state    data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/a0f52ab12eb4466bb6a76cc175923e62_0.csv')
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
                continue

            description = row[idescription]
            date        = row[idate]
            value       = row[ivalue]

            if description in stateFields:
                data['Colorado'][fieldMap[description]][formatDate(date)] = int(value)
    except HTTPError as e:
        printNow(now, '-- Error getting state data:', e.code)
        continue
    except Exception as e:
        printNow(now, '-- Error getting state data:', str(e))
        continue

    stateDates = sorted(list(set(data['Colorado']['Cases by Onset']) | set(data['Colorado']['Cases']) | set(data['Colorado']['Deaths'])))
    if stateDates[-1] != lastStateDate:
        updateData = True
        lastStateDate = stateDates[-1]
        lastUpdated[2] = now
        printNow('State    data updated to', lastStateDate[5:])

    # COVID19 Positivity Data from Clinical Laboratories
    # https://data-cdphe.opendata.arcgis.com/datasets/covid19-positivity-data-from-clinical-laboratories
    if logging:
        printNow('Getting    testing  data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/75d55e87fdc2430baf445fb29cec6de0_0.csv')
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
                continue

            Desc_     = row[iDesc_]
            Attr_Date = row[iAttr_Date]
            Metric    = row[iMetric]
            Value     = row[iValue]

            if Desc_ == 'Daily COVID-19 PCR Test Data From Clinical Laboratories' and Metric in ['Cumulative People Tested at CDPHE State Lab', 'Cumulative People Tested at Non-CDPHE (Commerical) Labs']:
                date = formatDate(Attr_Date)
                if date not in data['Colorado'][fieldMap[field]]:
                    data['Colorado'][fieldMap[field]][date] = 0
                data['Colorado'][fieldMap[field]][date] += int(Value)
    except HTTPError as e:
        printNow(now, '-- Error getting testing data:', e.code)
        continue
    except Exception as e:
        printNow(now, '-- Error getting testing data:', str(e))
        continue

    testDates = sorted(list(data['Colorado']['Tests']))
    if testDates[-1] != lastTestDate:
        updateData = True
        lastTestDate = testDates[-1]
        lastUpdated[3] = now
        printNow('Testing  data updated to', lastTestDate[5:])

    # CDPHE COVID19 County-Level Open Data Repository
    # https://data-cdphe.opendata.arcgis.com/datasets/cdphe-covid19-county-level-open-data-repository
    if logging:
        printNow('Getting    county   data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/efd7f5f77efa4122a70a0c5c405ce8be_0.csv')
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
                continue

            LABEL  = row[iLABEL]
            Desc_  = row[iDesc_]
            Metric = row[iMetric]
            Value  = row[iValue]
            Date   = row[iDate]

            if LABEL not in counties + ['Note', 'Unknown Or Pending County', 'Out Of State County', 'International']:
                LABEL = 'Other'
            if Desc_ == 'Cases of COVID-19 in Colorado by County' and Metric == 'Deaths': # errors in the data
                Desc_ = 'Deaths Among COVID-19 Cases in Colorado by County'
            date = formatDate(Date)
            if LABEL in counties and Desc_ in countyFields and Metric not in ['Percent of tests by PCR', 'Percent of tests by Serology']:
                if date not in data[LABEL][fieldMap[Desc_]]:
                    data[LABEL][fieldMap[Desc_]][date] = 0
                if 'E+' in Value:
                    Value = float(Value)
                data[LABEL][fieldMap[Desc_]][date] += int(Value)
    except HTTPError as e:
        printNow(now, '-- Error getting county data:', e.code)
        continue
    except Exception as e:
        printNow(now, '-- Error getting county data:', str(e))
        continue

    countyDates = []
    for county in counties:
        countyDates = sorted(list(set(countyDates) | set(data[county]['Cases']) | set(data[county]['Deaths']) | set(data[county]['Tests'])))
    if countyDates[-1] != lastCountyDate:
        updateData = True
        lastCountyDate = countyDates[-1]
        lastUpdated[4] = now
        printNow('County   data updated to', lastCountyDate[5:])

    # Colorado SARS-CoV-2 Variant Sentinel Surveillance
    # https://data-cdphe.opendata.arcgis.com/datasets/CDPHE::colorado-sars-cov-2-variant-sentinel-surveillance
    data['Colorado']['Other']['2020-03-01'] = 1
    week = datetime.date(2020,3,7)
    while week < datetime.date(2021,1,2):
        data['Colorado']['Other'][str(week)] = 1
        week += datetime.timedelta(days=7)

    if logging:
        printNow('Getting    variant  data')
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/8706d5ea533c483b93b3676c8abd6cb3_0.csv')
        variantData = reader(iterdecode(response, 'utf-8-sig'))
        if logging:
            printNow('Processing variant  data')

        newVariants = {}
        readFields = True
        for row in variantData:
            if readFields:
                ispecimen_collection_week = row.index('specimen_collection_week')
                ivariant_of_concern       = row.index('variant_of_concern')
                iproportion               = row.index('proportion')
                readFields = False
                continue

            specimen_collection_week = row[ispecimen_collection_week]
            variant_of_concern       = row[ivariant_of_concern]
            proportion               = row[iproportion]

            if variant_of_concern in variantFields:
                data['Colorado'][fieldMap[variant_of_concern]][saturday(specimen_collection_week)] = float(proportion)
            else:
                newVariants[variant_of_concern] = True
        if len(newVariants):
            printNow('New variants:', list(newVariants))

    except HTTPError as e:
        printNow(now, '-- Error getting variant data:', e.code)
        continue
    except Exception as e:
        printNow(now, '-- Error getting variant data:', str(e))
        continue

    variantDates = []
    for variant in variantHeaders:
        variantDates = sorted(list(set(variantDates) | set(data['Colorado'][variant])))
    latestVariantData = []
    for variant in variantHeaders:
        if variantDates[-1] in data['Colorado'][variant]:
            latestVariantData.append(data['Colorado'][variant][variantDates[-1]])
    if variantDates[-1] != lastVariantDate or latestVariantData != lastVariantData:
        updateData = True
        lastVariantDate = variantDates[-1]
        lastVariantData = latestVariantData
        lastUpdated[5] = now
        printNow('Variant  data updated to', lastVariantDate[5:])

    dates = sorted(list(set(vaccineDates) | set(hospitalDates) | set(stateDates) | set(testDates) | set(countyDates)))

    if not updateData:
        continue



    sheetData = [headers]

    def skip(field, i, j):
        badDates = {
            'First Dose'      : ['2021-08-05', '2022-01-29'],
            'All Doses'       : ['2021-08-05', '2022-01-29', '2022-04-22', '2022-06-27'],
            'Additional Doses': ['2022-09-02', '2022-09-05', '2022-09-06']
        }
        if field not in badDates:
            return False
        for date in badDates[field]:
            if i >= dates.index(date) and j < dates.index(date):
                return True
        return False

    def daily(region, field, i):
        if dates[i] not in data[region][field] or dates[i-1] not in data[region][field] or skip(field, i, i-1):
            return ''
        if i > 0:
            return max(0, data[region][field][dates[i]] - data[region][field][dates[i-1]])
        else:
            return max(0, data[region][field][dates[i]])

    def weekly(region, field, i):
        if dates[i] not in data[region][field] or dates[i-7] not in data[region][field] or skip(field, i, i-7):
            return ''
        if i > 6:
            return max(0, data[region][field][dates[i]] - data[region][field][dates[i-7]])
        else:
            return max(0, data[region][field][dates[i]])

    def strRound(num):
        if num == '':
            return ''

        return str(round(num/7, 3))

    if logging:
        printNow('Building output')

    for i in range(len(dates)):
        date = dates[i]
        # for field in ['First Dose', 'All Doses', 'Additional Doses']:
        #     if date in data['Colorado'][field]:
        #         printNow(field, date, data['Colorado'][field][date])

        if i>0:
            for field in stateFields:
                if fieldMap[field] in ['First Dose', 'All Doses', 'Additional Doses', 'Confirmed']:
                    continue
                if date not in data['Colorado'][fieldMap[field]] and dates[i-1] in data['Colorado'][fieldMap[field]]:
                    for j in range(i+1, len(dates)):
                        if dates[j] in data['Colorado'][fieldMap[field]]:
                            data['Colorado'][fieldMap[field]][date] = 0
                            break
            for field in countyFields:
                for county in counties:
                    if date not in data[county][fieldMap[field]] and dates[i-1] in data[county][fieldMap[field]]:
                        for j in range(i+1, len(dates)):
                            if dates[j] in data[county][fieldMap[field]]:
                                data[county][fieldMap[field]][date] = 0
                                break

        if i < dates.index('2020-03-01'):
            continue

        row = ['', date]

        for field in ['First Dose', 'All Doses', 'Additional Doses']:
            row.extend([str(daily('Colorado', field, i)), strRound(weekly('Colorado', field, i))])

            if field == 'Additional Doses' and date in ['2022-09-02', '2022-09-05']:
                row.append('')
            elif date in data['Colorado'][field]:
                row.append(str(round(100 * data['Colorado'][field][date] / 5763976, 3)))
            else:
                row.append('')

        if date in data['Colorado']['Confirmed'] or date == '2020-03-01' or date == dates[-1]:
            row.append(date)
        else:
            row.append('')

        for field in ['Confirmed', 'Under Investigation']:
            if date in data['Colorado'][field]:
                row.append(str(data['Colorado'][field][date]))
            else:
                row.append('')

        for field in ['Cases by Onset', 'Cases', 'Deaths', 'Tests']:
            row.extend([str(daily('Colorado', field, i)), strRound(weekly('Colorado', field, i))])

        if daily('Colorado', 'Tests', i) != '' and daily('Colorado', 'Tests', i) > 0:
            if daily('Colorado', 'Cases', i) != '':
                row.append(str(round(100 * daily('Colorado', 'Cases', i) / daily('Colorado', 'Tests', i) , 3)))
            else:
                row.append('0')
        else:
            row.append('')
        if weekly('Colorado', 'Tests', i) != '' and weekly('Colorado', 'Tests', i) > 0:
            if weekly('Colorado', 'Cases', i) != '':
                row.append(str(round(100 * weekly('Colorado', 'Cases', i) / weekly('Colorado', 'Tests', i), 3)))
            else:
                row.append('0')
        else:
            row.append('')

        for field in ['Cases', 'Deaths', 'Tests']:
            row.append('')
            for region in ['Colorado'] + counties:
                row.append(str(daily(region, field, i)))
            row.append('')
            for region in ['Colorado'] + counties:
                row.append(strRound(weekly(region, field, i)))

        row.append('')
        for region in ['Colorado'] + counties:
            if daily(region, 'Tests', i) != '' and daily(region, 'Tests', i) > 0:
                if daily(region, 'Cases', i) != '':
                    row.append(str(round(100 * daily(region, 'Cases', i) / daily(region, 'Tests', i) , 3)))
                else:
                    row.append('0')
            else:
                row.append('')
        row.append('')
        for region in ['Colorado'] + counties:
            if weekly(region, 'Tests', i) != '' and weekly(region, 'Tests', i) > 0:
                if weekly(region, 'Cases', i) != '':
                    row.append(str(round(100 * weekly(region, 'Cases', i) / weekly(region, 'Tests', i), 3)))
                else:
                    row.append('0')
            else:
                row.append('')

        sheetData.append(row)

    for i in range(len(lastUpdated)):
        sheetData[i+1][0] = '\'' + lastUpdated[i]



    variantData = [['Date', 'All Cases'] + variantHeaders + ['Sampled %']]
    for date in variantDates:
        row = [date, strRound(weekly('Colorado', 'Cases by Onset', dates.index(date)))]
        for variant in variantHeaders:
            if date in data['Colorado'][variant]:
                row.append(str(data['Colorado'][variant][date]))
            else:
                row.append('')
        if dates.index(date) < dates.index('2021-01-02'):
            row.append('0')
        else:
            sample = 0
            while True:
                sample += 1
                goodSample = True
                for variant in variantHeaders:
                    if date not in data['Colorado'][variant]:
                        continue
                    variantCount = floor((data['Colorado'][variant][date]-.0001)*sample)
                    while goodSample:
                        variantCount += 1
                        proportion = round(variantCount/sample, 4)
                        if proportion == data['Colorado'][variant][date]:
                            break
                        if proportion > data['Colorado'][variant][date]:
                            goodSample = False
                if goodSample:
                    row.append(str(round(sample/weekly('Colorado', 'Cases by Onset', dates.index(date))*100,3)))
                    break
        variantData.append(row)
    i = dates.index(variantDates[-1]) + 7
    while i+7 < len(dates) and dates[i+7] in data['Colorado']['Cases by Onset']:
        variantData.append([dates[i], strRound(weekly('Colorado', 'Cases by Onset', i))])
        i += 7
    variantData.append([dates[-1]])



    def updateSpreadsheet():
        try:
            service.spreadsheets().values().update(
                spreadsheetId = '1dfP3WLeU9T2InpIzNyo65R8d_e7NpPea9zKaldEdYRA',
                valueInputOption = 'USER_ENTERED',
                range = 'Data!A1:EN',
                body = dict(
                    majorDimension = 'ROWS',
                    values = sheetData,
                ),
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId = '1dfP3WLeU9T2InpIzNyo65R8d_e7NpPea9zKaldEdYRA',
                valueInputOption = 'USER_ENTERED',
                range = 'Data!JI1:KB',
                body = dict(
                    majorDimension = 'ROWS',
                    values = variantData,
                ),
            ).execute()
            return True
        except Exception as e:
            if str(e) == '[WinError 10053] An established connection was aborted by the software in your host machine':
                sleep(1)
                return updateSpreadsheet()
            else:
                printNow(now, '-- Error updating spreadsheet:', str(e))
                return False

    if not updateSpreadsheet():
        continue

    printNow(now, '-- Spreadsheet updated')
    updateData = False
    logging = False