from urllib.request import urlopen
from codecs import iterdecode
from csv import reader
from os import listdir

fieldMap = {
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
fields = ['Cases', 'Deaths', 'Tests']
stateFields = list(fieldMap)[:6]
countyFields = list(fieldMap)[6:]

data = {
    'Colorado': {
        'Confirmed': {},
        'Under Investigation': {},
        'Cases by Onset': {},
        'Cases': {},
        'Deaths': {},
        'Tests': {},
    }
}

headers = [
    'Date',
    'Confirmed Cases',
    'Under Investigation',
    'by date of illness onset', '       ',
    'by date reported'        , '',
    'by date of death'        , '',
    'Tests'                   , '       ',
    'Positive %'              , '',
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
    'Broomfield': {},
    'Other': {},
}

for county in counties:
    data[county] = {
        'Cases': {},
        'Deaths': {},
        'Tests': {},
    }

headers.extend(['Cases', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Weekly Cases', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Deaths', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Weekly Deaths', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Tests', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Weekly Tests', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Positive %', 'Colorado'])
for county in counties:
    headers.extend([county])
headers.extend(['Weekly Positive %', 'Colorado'])
for county in counties:
    headers.extend([county])

def formatDate(date):
    return date[6:10] + '-' + date[0:2] + '-' + date[3:5]

print('\nGetting state data', flush=True)
# CDPHE COVID19 State-Level Expanded Case Data
# https://data-cdphe.opendata.arcgis.com/datasets/cdphe-covid19-state-level-expanded-case-data
response = urlopen('https://opendata.arcgis.com/datasets/3421410bd96549538517b438d24be4da_0.csv')
stateData = reader(iterdecode(response, 'utf-8'))
print('Processing state data', flush=True)
for row in stateData:
    field = row[3]
    if field in stateFields:
        data['Colorado'][fieldMap[field]][formatDate(row[4])] = int(row[6])

dates = sorted(list(set(data['Colorado']['Cases by Onset']) | set(data['Colorado']['Cases'])))

# https://drive.google.com/drive/folders/1bjQ7LnhU8pBR3Ly63341bCULHFqc7pMw
for filename in listdir():
    if filename[:25] == 'covid19_hospital_data_202' and filename[-4:] == '.csv':
        hospitalFilename = filename
if hospitalFilename[22:32] < dates[-1]:
    print('\nUpdate hospital data')
    exit()
with open(hospitalFilename) as file:
    hospitalData = reader(file)
    print('Processing hospital data', flush=True)
    for row in hospitalData:
        field = row[5]
        if row[1] == 'Hospital Level' and row[2] == 'Currently Hospitalized' and row[3] == 'Colorado' and field in stateFields:
            data['Colorado'][fieldMap[field]][row[4]] = int(row[6])

print('Getting county data', flush=True)
# CDPHE COVID19 County-Level Open Data Repository
# https://data-cdphe.opendata.arcgis.com/datasets/cdphe-covid19-county-level-open-data-repository
response = urlopen('https://opendata.arcgis.com/datasets/890ee7d8bb42419bb745c03eb76a2ba5_0.csv')
countyData = reader(iterdecode(response, 'utf-8'))
print('Processing county data', flush=True)
for row in countyData:
    county = row[2]
    if county not in counties and county not in ['Note', 'Unknown Or Pending County', 'Out Of State County', 'International']:
        county = 'Other'

    field = row[6]
    if field == 'Cases of COVID-19 in Colorado by County' and row[7] == 'Deaths': # errors in the data
        field = 'Deaths Among COVID-19 Cases in Colorado by County'

    date = formatDate(row[10])

    if county in counties and field in countyFields and row[7] not in ['Percent of tests by PCR', 'Percent of tests by Serology']:
        if date not in data[county][fieldMap[field]]:
            data[county][fieldMap[field]][date] = 0
        data[county][fieldMap[field]][date] += int(row[8])

print('Getting testing data', flush=True)
# COVID19 Positivity Data from Clinical Laboratories
# https://data-cdphe.opendata.arcgis.com/datasets/-covid19-positivity-data-from-clinical-laboratories
response = urlopen('https://opendata.arcgis.com/datasets/51839032444c40a9b4430b4d6a37a6d3_0.csv')
testingData = reader(iterdecode(response, 'utf-8'))
print('Processing testing data', flush=True)
field = 'Cumulative People Tested at Lab'
for row in testingData:
    if row[1] == 'Daily COVID-19 PCR Test Data From Clinical Laboratories' and row[3] in ['Cumulative People Tested at CDPHE State Lab', 'Cumulative People Tested at Non-CDPHE (Commerical) Labs']:
        date = formatDate(row[2])
        if date not in data['Colorado'][fieldMap[field]]:
            data['Colorado'][fieldMap[field]][date] = 0
        data['Colorado'][fieldMap[field]][date] += int(row[4])

tsvData = '\t'.join(headers) + '\n'

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

print('Building output\n', flush=True)

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
    if date in data['Colorado']['Confirmed']:
        row += str(data['Colorado']['Confirmed'][date])
    row += '\t'
    if date in data['Colorado']['Under Investigation']:
        row += str(data['Colorado']['Under Investigation'][date])
    row += '\t'

    for field in ['Cases by Onset'] + fields:
        row += str(daily('Colorado', field, i)) + '\t' + strRound(weekly('Colorado', field, i)) + '\t'

    if daily ('Colorado', 'Tests', i) != '' and daily('Colorado', 'Tests', i) > 0:
        if daily('Colorado', 'Cases', i) != '':
            row += str(round(100 * daily('Colorado', 'Cases', i) / daily('Colorado', 'Tests', i) , 3))
        else:
            row += '0'
    row += '\t'
    if weekly('Colorado', 'Tests', i) != '' and weekly('Colorado', 'Tests', i) > 0:
        if weekly('Colorado', 'Cases', i) != '':
            row += str(round(100 * weekly('Colorado', 'Cases', i) / weekly('Colorado', 'Tests', i), 3))
        else:
            row += '0'
    row += '\t'

    for field in fields:
        row += '\t'
        for region in ['Colorado'] + list(counties):
            row += str(daily(region, field, i)) + '\t'
        row += '\t'
        for region in ['Colorado'] + list(counties):
            row += strRound(weekly(region, field, i)) + '\t'

    row += '\t'
    for region in ['Colorado'] + list(counties):
        if daily(region, 'Tests', i) != '' and daily(region, 'Tests', i) > 0:
            if daily(region, 'Cases', i) != '':
                row += str(round(100 * daily(region, 'Cases', i) / daily(region, 'Tests', i) , 3))
            else:
                row += '0'
        row += '\t'
    row += '\t'
    for region in ['Colorado'] + list(counties):
        if weekly(region, 'Tests', i) != '' and weekly(region, 'Tests', i) > 0:
            if weekly(region, 'Cases', i) != '':
                row += str(round(100 * weekly(region, 'Cases', i) / weekly(region, 'Tests', i), 3))
            else:
                row += '0'
        row += '\t'

    tsvData += row[:-1] + '\n'

with open('data.tsv', 'w') as newFile:
    newFile.write(tsvData)
print(dates[-1])