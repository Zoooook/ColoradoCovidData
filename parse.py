from urllib.request import urlopen
from codecs import iterdecode
from csv import reader
from os import listdir

data = {
    'Confirmed COVID-19': {},
    'COVID-19 Persons Under Investigation': {},
    'Cumulative COVID-19 Cases in Colorado by Date of Illness Onset': {},
    'Cumulative COVID-19 Cases in Colorado by Date Reported to the State': {},
    'Cumulative Deaths Among COVID-19 Cases in Colorado by Date of Death': {},
    'Cumulative People Tested at Lab': {},
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
    data[county + ' Cases of COVID-19 in Colorado by County'] = {}
    data[county + ' Deaths Among COVID-19 Cases in Colorado by County'] = {}
    data[county + ' Total COVID-19 Tests Performed in Colorado by County'] = {}
    headers.extend([county + ' Cases', '', county + ' Deaths', '', 'Tests', '       ', 'Positive %', ''])

fields = list(data)

def formatDate(date):
    return date[6:10] + '-' + date[0:2] + '-' + date[3:5]

response = urlopen('https://opendata.arcgis.com/datasets/331ca20801e545c7a656158aaad6f8af_0.csv')
stateData = reader(iterdecode(response, 'utf-8'))
for row in stateData:
    if row[2] in fields:
        data[row[2]][formatDate(row[3])] = int(row[5])
print()

response = urlopen('https://opendata.arcgis.com/datasets/52fb11a8a07f49c1b28335a9de9ba99f_0.csv')
countyData = reader(iterdecode(response, 'utf-8'))
for row in countyData:
    if row[1] not in counties and row[1] not in ['Note', 'Unknown Or Pending County', 'Out Of State County', 'International']:
        row[1] = 'Other'
    if row[5] == 'Cases of COVID-19 in Colorado by County' and row[6] == 'Deaths': # errors in the data
        row[5] = 'Deaths Among COVID-19 Cases in Colorado by County'
    key = row[1] + ' ' + row[5]
    date = formatDate(row[9])

    if row[1] in counties and key in fields and row[6] not in ['Percent of tests by PCR', 'Percent of tests by Serology']:
        if date not in data[key]:
            data[key][date] = 0
        data[key][date] += int(row[7])

response = urlopen('https://opendata.arcgis.com/datasets/ca2c4b063f494506a1047d9783789ef7_0.csv')
testingData = reader(iterdecode(response, 'utf-8'))
key = 'Cumulative People Tested at Lab'
for row in testingData:
    if row[2] == 'Daily COVID-19 PCR Test Data From Clinical Laboratories' and row[4] in ['Cumulative People Tested at CDPHE State Lab', 'Cumulative People Tested at Non-CDPHE (Commerical) Labs']:
        date = formatDate(row[3])
        if date not in data[key]:
            data[key][date] = 0
        data[key][date] += int(row[5])

for filename in listdir():
    if filename[:25] == 'covid19_hospital_data_202' and filename[-4:] == '.csv':
        hospitalFilename = filename
with open(hospitalFilename) as file:
    hospitalData = reader(file)
    for row in hospitalData:
        if row[1] == 'Hospital Level' and row[2] == 'Currently Hospitalized' and row[3] == 'Colorado' and row[5] in fields:
            data[row[5]][row[4]] = int(row[6])

dates = sorted(list(set(data['Cumulative COVID-19 Cases in Colorado by Date of Illness Onset']) | set(data['Cumulative COVID-19 Cases in Colorado by Date Reported to the State'])))
if hospitalFilename[22:32] < dates[-1]:
    print('Update hospital data')
    exit()
print(dates[-1])

tsvData = '\t'.join(headers) + '\n'

ratio = {}
for i in range(len(dates)):
    date = dates[i]
    row = date + '\t'

    for field in fields:
        if date not in data[field]:
            data[field][date] = 0
        today = data[field][date]

        if field in ['Confirmed COVID-19', 'COVID-19 Persons Under Investigation']:
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

        if field == 'Cumulative COVID-19 Cases in Colorado by Date Reported to the State':
            ratio['Cases'] = {
                'daily': daily,
                'weekly': today - lastweek,
            }

        if field == 'Cumulative People Tested at Lab':
            ratio['Tests'] = {
                'daily': daily,
                'weekly': today - lastweek,
            }

            if ratio['Tests']['daily'] > 0:
                daily = str(round(100 * ratio['Cases']['daily'] / ratio['Tests']['daily'] , 3))
            else:
                daily = ''
            if ratio['Tests']['weekly'] > 0:
                weekly = str(round(100 * ratio['Cases']['weekly'] / ratio['Tests']['weekly'], 3))
            else:
                weekly = ''
            row += daily + '\t' + weekly + '\t'

        if ' '.join(words[:2]) in ['El Paso']:
            words = [' '.join(words[:2])] + words[2:]
        county = words[0]
        if county in counties:
            countyField = ' '.join(words[1:])

            if countyField == 'Cases of COVID-19 in Colorado by County':
                counties[county]['Cases'] = {
                    'daily': daily,
                    'weekly': today - lastweek,
                }

            if countyField == 'Total COVID-19 Tests Performed in Colorado by County':
                counties[county]['Tests'] = {
                    'daily': daily,
                    'weekly': today - lastweek,
                }

                if counties[county]['Tests']['daily'] > 0:
                    daily  = str(round(100 * counties[county]['Cases']['daily']  / counties[county]['Tests']['daily'] , 3))
                else:
                    daily = ''
                if counties[county]['Tests']['weekly'] > 0:
                    weekly = str(round(100 * counties[county]['Cases']['weekly'] / counties[county]['Tests']['weekly'], 3))
                else:
                    weekly = ''
                row += daily + '\t' + weekly + '\t'

    tsvData += row[:-1] + '\n'

with open('data.tsv', 'w') as newFile:
    newFile.write(tsvData)