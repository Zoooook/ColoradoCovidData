from urllib.request import urlopen
from codecs import iterdecode
import csv

response = urlopen('https://opendata.arcgis.com/datasets/90cc1ace62254550879f18cf94ca216b_0.csv')
file = csv.reader(iterdecode(response, 'utf-8'))

stateData = []
for line in file:
    stateData.append(line)

response = urlopen('https://opendata.arcgis.com/datasets/1456d8d43486449292e5784dcd9ce4a7_0.csv')
file = csv.reader(iterdecode(response, 'utf-8'))

countyData = []
for line in file:
    countyData.append(line)

response = urlopen('https://opendata.arcgis.com/datasets/ca2c4b063f494506a1047d9783789ef7_0.csv')
file = csv.reader(iterdecode(response, 'utf-8'))

testingData = []
for line in file:
    testingData.append(line)

hospitalizationData = []
with open('Public_ Patients v discharged_crosstab.csv') as file:
    for line in file.readlines():
        row = ''
        for i in range(1, len(line)-1, 2):
            row += line[i]
        hospitalizationData.append(row.split('\t'))

data = {
    'Cases of COVID-19 in Colorado by Date of Illness Onset': {},
    'Cases of COVID-19 in Colorado by Date Reported to the State': {},
    'Count of people tested by lab': {},
    'Hospitalized Cases of COVID-19 in Colorado by Date of Illness Onset': {},
    'Hospitalized Cases of COVID-19 in Colorado by Date Reported to the State': {},
    'Currently hospitalized for confirmed COVID-19': {},
    'Currently hospitalized as COVID-19 PUIs': {},
    'Deaths From COVID-19 in Colorado by Date of Illness': {},
    'Deaths From COVID-19 in Colorado by Date Reported to the State': {},
    'Deaths From COVID-19 in Colorado by Date of Death': {},
}

headers = [
    'Date',
    'by date of illness onset', '       ',
    'by date reported'        , '',
    'Tests'                   , '       ',
    'Positive %'              , '',
    'by date of illness onset', '       ',
    'by date reported'        , '',
    'Confirmed Cases',
    'Under Investigation',
    'by date of illness onset', '       ',
    'by date reported'        , '       ',
    'by date of death'        , '',
]

counties = {
    'Denver': {},
    'El Paso': {},
    'Arapahoe': {},
    'Jefferson': {},
    'Adams': {},
    'Larimer': {},
    'Douglas': {},
    'Boulder': {},
    'Weld': {},
    'Pueblo': {},
    'Mesa': {},
    'Other': {},
}

for county in counties:
    data[county + ' Colorado Case Counts by County'] = {}
    data[county + ' Total COVID-19 Tests Performed in Colorado by County'] = {}
    data[county + ' Number of Deaths by County'] = {}
    headers.extend([county + ' Cases', '', 'Tests', '       ', 'Positive %', '', county + ' Deaths', ''])

fields = list(data)

def formatDate(date):
    return date[6:10] + '-' + date[0:2] + '-' + date[3:5]

for row in stateData[1:]:
    if row[0] == 'Colorado' and row[2][21:] in fields:
        data[row[2][21:]][formatDate(row[4])] = int(row[6])

for row in countyData[1:]:
    if row[1] not in counties and row[1] not in ['Note', 'Unknown Or Pending County', 'Out Of State County', 'International']:
        row[1] = 'Other'
    key = row[1] + ' ' + row[5]
    date = formatDate(row[9])

    if row[1] in counties and key in fields and row[6] not in ['Percent of tests by PCR', 'Percent of tests by Serology']:
        if date not in data[key]:
            data[key][date] = 0
        data[key][date] += int(row[7])

for row in testingData[1:]:
    if row[4] in ['Count of people tested by CDPHE lab', 'Count of people tested by non-CDPHE (commercial) lab']:
        date = formatDate(row[3])
        if date not in data['Count of people tested by lab']:
            data['Count of people tested by lab'][date] = 0
        data['Count of people tested by lab'][date] += int(row[5])

dates = sorted(list(data['Cases of COVID-19 in Colorado by Date of Illness Onset']))

for i in range(len(dates)):
    date = dates[i]
    if i > 0:
        old = data['Count of people tested by lab'][dates[i-1]]
    else:
        old = 0
    if date in data['Count of people tested by lab']:
        new = data['Count of people tested by lab'][date]
    else:
        new = 0
    data['Count of people tested by lab'][date] = old + new

monthMap = {
    'January'  : '01',
    'February' : '02',
    'March'    : '03',
    'April'    : '04',
    'May'      : '05',
    'June'     : '06',
    'July'     : '07',
    'August'   : '08',
    'September': '09',
    'October'  : '10',
    'November' : '11',
    'December' : '12',
}

def formatDateString(date):
    parts = date.split(' ')
    return parts[2] + '-' + monthMap[parts[0]] + '-' + parts[1][:-1].zfill(2)

for row in hospitalizationData:
    if len(row) > 1:
        if row[3]:
            data['Currently hospitalized for confirmed COVID-19'][formatDateString(row[0])] = int(row[3])
        if row[5]:
            data['Currently hospitalized as COVID-19 PUIs'][formatDateString(row[0])] = int(row[5])

tsvData = '\t'.join(headers) + '\n'

ratio = {}
for i in range(len(dates)):
    date = dates[i]
    row = date + '\t'

    for field in fields:
        if date not in data[field]:
            data[field][date] = 0
        today = data[field][date]

        if field in ['Currently hospitalized for confirmed COVID-19', 'Currently hospitalized as COVID-19 PUIs']:
            row += str(today) + '\t'
            continue

        if i > 0:
            yesterday = data[field][dates[i-1]]
        else:
            yesterday = 0
        daily = max(0, today - yesterday)

        if i > 6:
            lastweek = data[field][dates[i-7]]
        else:
            lastweek = 0
        weekly = round(max(0, today - lastweek) / 7, 3)

        row += str(daily) + '\t' + str(weekly) + '\t'

        words = field.split(' ')

        if field in ['Cases of COVID-19 in Colorado by Date Reported to the State', 'Count of people tested by lab']:
            ratio[words[0]] = {
                'daily': daily,
                'weekly': today - lastweek,
            }
            if words[0] == 'Count':
                if ratio['Count']['daily'] > 0:
                    daily  = str(round(100 * ratio['Cases']['daily']  / ratio['Count']['daily'] , 3))
                else:
                    daily = ''
                if ratio['Count']['weekly'] > 0:
                    weekly = str(round(100 * ratio['Cases']['weekly'] / ratio['Count']['weekly'], 3))
                else:
                    weekly = ''
                row += daily + '\t' + weekly + '\t'

        if ' '.join(words[:2]) in ['El Paso']:
            words = [' '.join(words[:2])] + words[2:]
        if words[0] in counties and words[3] in ['Counts', 'Tests']:
            counties[words[0]][words[3]] = {
                'daily': daily,
                'weekly': today - lastweek,
            }
            if words[3] == 'Tests':
                if counties[words[0]]['Tests']['daily'] > 0:
                    daily  = str(round(100 * counties[words[0]]['Counts']['daily']  / counties[words[0]]['Tests']['daily'] , 3))
                else:
                    daily = ''
                if counties[words[0]]['Tests']['weekly'] > 0:
                    weekly = str(round(100 * counties[words[0]]['Counts']['weekly'] / counties[words[0]]['Tests']['weekly'], 3))
                else:
                    weekly = ''
                row += daily + '\t' + weekly + '\t'

    tsvData += row[:-1] + '\n'

with open('data.tsv', 'w') as newFile:
    newFile.write(tsvData)