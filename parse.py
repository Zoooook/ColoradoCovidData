from pickle import load
from googleapiclient.discovery import build
from csv import reader
import json
from time import sleep
import datetime
from math import floor
from urllib.request import urlopen
from codecs import iterdecode
from urllib.error import HTTPError
from copy import deepcopy

with open('token.pickle', 'rb') as token:
    creds = load(token)
service = build('sheets', 'v4', credentials=creds)

with open('api.key', 'r') as key:
    apiKey = key.read()
drive = build('drive', 'v3', developerKey=apiKey)

fieldMap = {
    'Confirmed COVID-19'                                                 : 'Confirmed',
    'COVID-19 Persons Under Investigation'                               : 'Under Investigation',
    'Cumulative COVID-19 Cases in Colorado by Date of Illness Onset'     : 'Cases by Onset',
    'Cumulative COVID-19 Cases in Colorado by Date Reported to the State': 'Cases',
    'Cumulative Deaths Among COVID-19 Cases in Colorado by Date of Death': 'Deaths',

    'Other'            : 'Other',
    'B.1.1.7 Alpha'    : 'B.1.1.7',
    'B.1.351 Beta'     : 'B.1.351',
    'P.1 Gamma'        : 'P.1',
    'B.1.617.2 Delta'  : 'B.1.617.2',
    'AY.2 Delta'       : 'AY.2',
    'AY.1 Delta'       : 'AY.1',
    'B.1.429'          : 'B.1.429',
    'B.1.427'          : 'B.1.427',
    'B.1.1.529 Omicron': 'BA.1',
    'BA.2 Omicron'     : 'BA.2',
    'BA.2.12.1 Omicron': 'BA.2.12.1',
    'BA.4 Omicron'     : 'BA.4',
    'BA.5 Omicron'     : 'BA.5',
    'BA.4.6 Omicron'   : 'BA.4.6',
    'BF.7 Omicron'     : 'BF.7',
    'BA.2.75 Omicron'  : 'BA.2.75',
    'BA.2.75.2 Omicron': 'BA.2.75.2',
    'BQ.1 Omicron'     : 'BQ.1',
    'BQ.1.1 Omicron'   : 'BQ.1.1',
    'BA.5.2.6 Omicron' : 'BA.5.2.6',
    'BN.1 Omicron'     : 'BN.1',
    'BF.11 Omicron'    : 'BF.11',
    'CH.1.1 Omicron'   : 'CH.1.1',
    'XBB Omicron'      : 'XBB',
    'XBB.1.5 Omicron'  : 'XBB.1.5',
    'XBB.1.5.1 Omicron': 'XBB.1.5.1',
    'XBB.1.9.1 Omicron': 'XBB.1.9.1',
    'XBB.1.9.2 Omicron': 'XBB.1.9.2',
    'XBB.1.16 Omicron' : 'XBB.1.16',
    'FD.2 Omicron'     : 'FD.2',
    'XBB.2.3 Omicron'  : 'XBB.2.3',
}
stateFields = list(fieldMap)[:5]
variantFields = list(fieldMap)[5:]

headers = [
    'Last Updated'            ,
    'Date'                    , '', '',
    'First Dose'              , '%       ', '', '',
    'All Doses'               , '%       ', '', '',
    'First Booster'           , '%       ', '', '',
    'Second Booster'          , '%       ', '', '',
    'Omicron Doses'           , '%       ',
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

firstRun              = True
lastRun               = ''
lastHospitalDate      = ''
lastStateDate         = ''
lastVariantDate       = ''
lastVariantReportDate = ''
lastVariantData       = {}
lastUpdated           = ['', '', '']

vaccineData = {}
vaccineDates = []
with open('vaccine_data.csv') as file:
    vaccineCsv = reader(file)
    for row in vaccineCsv:
        vaccineDates.append(row[0])
        vaccineData[row[0]] = row[1:]

testingData = {}
testDates = []
with open('testing_data.csv') as file:
    testingCsv = reader(file)
    for row in testingCsv:
        testDates.append(row[0])
        testingData[row[0]] = row[1:]

countyData = {}
countyDates = []
with open('county_data.csv') as file:
    countyCsv = reader(file)
    for row in countyCsv:
        countyDates.append(row[0])
        countyData[row[0]] = row[1:]

with open('variant_data.json') as file:
    variantJson = json.load(file)

while True:
    if not firstRun:
        sleep(3)
    firstRun = False

    now = str(datetime.datetime.now())[:16]

    thisRun = now[:15] + str(floor(int(now[15])/5)*5)
    if thisRun == lastRun:
        continue
    lastRun = thisRun

    if now[-5:] == '00:00':
        printNow('')

    for i in range(len(lastUpdated)):
        if not lastUpdated[i]:
            lastUpdated[i] = now

    data = {
        'Colorado': {
            'First Dose'         : {},
            'All Doses'          : {},
            'First Booster'      : {},
            'Second Booster'     : {},
            'Omicron Doses'      : {},
            'Confirmed'          : {},
            'Under Investigation': {},
            'Cases by Onset'     : {},
            'Cases'              : {},
            'Deaths'             : {},
            'Tests'              : {},
        }
    }

    def formatDate(date):
        return date[6:10] + '-' + date[0:2] + '-' + date[3:5]

    def nextSaturday(date):
        return str(datetime.date(int(date[0:4]), int(date[5:7]), int(date[8:10])) + datetime.timedelta(days=13))



    def parseHospitalData(hospitalData):
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
                data['Colorado'][fieldMap[metric]][date[:10].replace('/','-')] = int(value)

    # CDPHE COVID19 Hospital Data
    # https://data-cdphe.opendata.arcgis.com/datasets/CDPHE::cdphe-covid19-hospital-data
    def getHospitalData():
        try:
            response = urlopen('https://opendata.arcgis.com/datasets/bdf90453c5ca46338c51143a2edd810d_0.csv')
            hospitalData = reader(iterdecode(response, 'utf-8-sig'))

            parseHospitalData(hospitalData)
            with open('covid19_hospital_data_2022-03-15.csv') as file:
                hospitalData = reader(file)
                parseHospitalData(hospitalData)
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

    if not getHospitalData():
        continue

    hospitalDates = sorted(list(set(data['Colorado']['Confirmed']) | set(data['Colorado']['Under Investigation'])))
    if hospitalDates[-1] != lastHospitalDate:
        updateData = True
        lastHospitalDate = hospitalDates[-1]
        lastUpdated[0] = now
        printNow('Hospital data updated to', lastHospitalDate[5:])

    # CDPHE COVID19 State-Level Expanded Case Data
    # https://data-cdphe.opendata.arcgis.com/datasets/cdphe-covid19-state-level-expanded-case-data
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/cc2c6500f01e460690a1a25aa41528d3_0.csv')
        stateData = reader(iterdecode(response, 'utf-8-sig'))

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
                data['Colorado'][fieldMap[description]][date[:10].replace('/','-')] = int(value)
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
        lastUpdated[1] = now
        printNow('State    data updated to', lastStateDate[5:])

    # Colorado SARS-CoV-2 Variant Sentinel Surveillance
    # https://data-cdphe.opendata.arcgis.com/datasets/CDPHE::cdphe-covid19-sentinel-surveillance
    try:
        response = urlopen('https://opendata.arcgis.com/datasets/2b58e4b5263342c6a6d75f3395bc18a9_0.csv')
        variantData = reader(iterdecode(response, 'utf-8-sig'))

        newVariantData = {}
        readFields = True
        for row in variantData:
            if readFields:
                iweek_start   = row.index('week_start')
                ivariant      = row.index('variant')
                inumber       = row.index('number')
                ipublish_date = row.index('publish_date')
                readFields = False
                continue

            week_start   = row[iweek_start]
            variant      = row[ivariant].replace('\n', ' ')
            number       = row[inumber]
            publish_date = row[ipublish_date][:10].replace('/','-')

            two_week_end = nextSaturday(week_start)
            if two_week_end >= '2023-04-01':
                if two_week_end not in newVariantData:
                    newVariantData[two_week_end] = {'Total': 0}
                newVariantData[two_week_end]['Total'] += int(number)

                if variant in variantFields:
                    newVariantData[two_week_end][fieldMap[variant]] = int(number)
                else:
                    newVariantData[two_week_end][variant] = int(number)
    except HTTPError as e:
        printNow(now, '-- Error getting variant data:', e.code)
        continue
    except Exception as e:
        printNow(now, '-- Error getting variant data:', str(e))
        continue

    if lastVariantReportDate != publish_date:
        lastVariantReportDate = publish_date
        printNow('Variant report published for', publish_date[5:])
    if newVariantData != lastVariantData:
        updateData = True
        lastVariantData = deepcopy(newVariantData)
        lastUpdated[2] = now
        printNow('Variant  data updated to', sorted(list(set(newVariantData)))[-1][5:], '<--------------------------------')

        with open('variant_data.json') as file:
            variantJson = json.load(file)
        for date in newVariantData:
            variantJson[date] = newVariantData[date]

        newVariants = {}
        for date in variantJson:
            for variant in variantJson[date]:
                if variant != 'Total' and variant not in variantHeaders:
                    if variant in variantFields:
                        variantJson[date][fieldMap[variant]] = variantJson[date][variant]
                        del variantJson[date][variant]
                    else:
                        newVariants[variant] = True
        if len(newVariants):
            printNow('New variants:', list(newVariants))

        with open('variant_data.json', 'w') as file:
            json.dump(variantJson, file)
        variantDates = sorted(list(set(variantJson)))

    dates = sorted(list(set(vaccineDates) | set(hospitalDates) | set(stateDates) | set(testDates) | set(countyDates)))

    if not updateData:
        continue



    def daysDiff(region, field, i, days):
        if dates[i] not in data[region][field] or dates[i-days] not in data[region][field]:
            return ''
        if i > days - 1:
            return max(0, data[region][field][dates[i]] - data[region][field][dates[i-days]])
        else:
            return max(0, data[region][field][dates[i]])

    def strAverage(num, days):
        if num == '':
            return ''

        return str(round(num/days, 3))

    sheetData = [headers]

    for i in range(len(dates)):
        date = dates[i]

        if i>0:
            for field in stateFields:
                if fieldMap[field] != 'Confirmed' and date not in data['Colorado'][fieldMap[field]] and dates[i-1] in data['Colorado'][fieldMap[field]]:
                    for j in range(i+1, len(dates)):
                        if dates[j] in data['Colorado'][fieldMap[field]]:
                            data['Colorado'][fieldMap[field]][date] = 0
                            break

        if date < '2020-03-01':
            continue

        row = ['', date]

        if date in vaccineDates:
            row.extend(vaccineData[date])
        else:
            row.extend(['']*20)

        if date in data['Colorado']['Confirmed'] or date == '2020-03-01' or date == dates[-1]:
            row.append(date)
        else:
            row.append('')

        for field in ['Confirmed', 'Under Investigation']:
            if date in data['Colorado'][field]:
                row.append(str(data['Colorado'][field][date]))
            else:
                row.append('')

        for field in ['Cases by Onset', 'Cases', 'Deaths']:
            row.extend([str(daysDiff('Colorado', field, i, 1)), strAverage(daysDiff('Colorado', field, i, 7), 7)])

        if date in testDates:
            row.extend(testingData[date])
        else:
            row.extend(['']*4)

        if date in countyDates:
            row.extend(countyData[date])
        else:
            row.extend(['']*120)

        sheetData.append(row)

    for i in range(len(lastUpdated)):
        sheetData[i+1][0] = '\'' + lastUpdated[i]

    variantData = [['Date', 'All Cases', 'Sampled %', 'Total'] + variantHeaders]
    for date in variantDates:
        if date < '2023-04-01':
            days = 7
        else:
            days = 14
        allCases = daysDiff('Colorado', 'Cases by Onset', dates.index(date), days)

        row = [
            date,
            strAverage(allCases, days),
            str(round(variantJson[date]['Total']/allCases*100, 3)),
            str(variantJson[date]['Total']),
        ]

        for variant in variantHeaders:
            if variant in variantJson[date]:
                row.append(str(variantJson[date][variant]))
            else:
                row.append('')

        variantData.append(row)
    variantData.append([dates[-1]])



    def updateSpreadsheet():
        try:
            service.spreadsheets().values().update(
                spreadsheetId = '1dfP3WLeU9T2InpIzNyo65R8d_e7NpPea9zKaldEdYRA',
                valueInputOption = 'USER_ENTERED',
                range = 'Data!A1:EY',
                body = dict(
                    majorDimension = 'ROWS',
                    values = sheetData,
                ),
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId = '1dfP3WLeU9T2InpIzNyo65R8d_e7NpPea9zKaldEdYRA',
                valueInputOption = 'USER_ENTERED',
                range = 'Data!JT1:LC',
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