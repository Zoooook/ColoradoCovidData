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

data = {
    'Cases of COVID-19 in Colorado by Date of Illness Onset': {},
    'Cases of COVID-19 in Colorado by Date Reported to the State': {},
    'Hospitalized Cases of COVID-19 in Colorado by Date of Illness Onset': {},
    'Hospitalized Cases of COVID-19 in Colorado by Date Reported to the State': {},
    'Deaths From COVID-19 in Colorado by Date of Illness': {},
    'Deaths From COVID-19 in Colorado by Date Reported to the State': {},
    'Deaths From COVID-19 in Colorado by Date of Death': {},
}

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

headers = [
    'Date',
    'Cases by Onset'    , 'Weekly',
    'Cases by Reported' , 'Weekly',
    'Hosps by Onset'    , 'Weekly',
    'Hosps by Reported' , 'Weekly',
    'Deaths by Onset'   , 'Weekly',
    'Deaths by Reported', 'Weekly',
    'Deaths by Date'    , 'Weekly',
]

for county in counties:
    data[county + ' Colorado Case Counts by County'] = {}
    data[county + ' Total COVID-19 Tests Performed in Colorado by County'] = {}
    data[county + ' Number of Deaths by County'] = {}
    headers.extend([county + ' Cases', 'Weekly', county + ' Tests', 'Weekly', county + ' Positivity', 'Weekly', county + ' Deaths', 'Weekly'])

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

dates = sorted(list(data['Cases of COVID-19 in Colorado by Date of Illness Onset']))

tsvData = '\t'.join(headers) + '\n'

for i in range(len(dates)):
    date = dates[i]
    row = date + '\t'

    for field in fields:
        if date not in data[field]:
            data[field][date] = 0
        today = data[field][date]

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
        if ' '.join(words[:2]) in ['El Paso']:
            words = [' '.join(words[:2])] + words[2:]
        if words[0] in counties and words[3] in ['Counts', 'Tests']:
            counties[words[0]][words[3]] = {
                'daily': daily,
                'weekly': weekly,
            }
            if words[3] == 'Tests':
                if counties[words[0]]['Tests']['daily'] > 0:
                    daily  = str(round(100 * counties[words[0]]['Counts']['daily']  / counties[words[0]]['Tests']['daily'] , 2))
                else:
                    daily = ''
                if counties[words[0]]['Tests']['weekly'] > 0:
                    weekly = str(round(100 * counties[words[0]]['Counts']['weekly'] / counties[words[0]]['Tests']['weekly'], 2))
                else:
                    weekly = ''
                row += daily + '\t' + weekly + '\t'

    tsvData += row[:-1] + '\n'

with open('data.tsv', 'w') as newFile:
    newFile.write(tsvData)
