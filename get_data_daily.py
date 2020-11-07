#!/usr/bin/env python
# coding: utf-8

import json
import pandas as pd
import boto3
import requests
from datetime import datetime, timedelta
import ssl
from geopy.geocoders import Nominatim
import holidays

ssl._create_default_https_context = ssl._create_unverified_context

province_abbs = {
    'Baden-Württemberg': 'BW',
    'Bayern': 'BY',
    'Berlin': 'BE',
    'Brandenburg': 'BB',
    'Bremen': 'HB',
    'Hamburg': 'HH',
    'Hessen': 'HE',
    'Mecklenburg-Vorpommern': 'MV',
    'Niedersachsen': 'NI',
    'Nordrhein-Westfalen': 'NW',
    'Rheinland-Pfalz': 'RP',
    'Saarland': 'SL',
    'Sachsen': 'SN',
    'Sachsen-Anhalt': 'ST',
    'Schleswig-Holstein': 'SH',
    'Thüringen': 'TH'
}


def get_data(start_date):
    locations_path = "Counterlist-DE.csv"
    locations_df = pd.read_csv(locations_path, sep='\t')
    data_sets = []

    end_date = start_date + timedelta(days=1)
    start_day, start_month, start_year = start_date.day, start_date.month, start_date.year
    end_day, end_month, end_year = end_date.day, end_date.month, end_date.year

    for index, row in locations_df.iterrows():
        url = 'http://www.eco-public.com/ParcPublic/CounterData'

        # start get bike count data
        # ------------------------------------------------
        pratiques = ""
        if hasattr(row, 'pratiques'):
            pratiques = "&pratiques=" + row.pratiques
        body = "idOrganisme=4586&idPdc={}&fin={}%2F{}%2F{}&debut={}%2F{}%2F{}&interval=4&pratiques={}".format(row.idPdc,
                                                                                                              end_day,
                                                                                                              end_month,
                                                                                                              end_year,
                                                                                                              start_day,
                                                                                                              start_month,
                                                                                                              start_year,
                                                                                                              pratiques)

        headers = {
            "Connection": "keep-alive",
            "Accept": "text/plain, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "http://www.eco-public.com",
            "Referer": "http://www.eco-public.com/ParcPublic/?id=888",
            "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cookie": "_ga=GA1.2.2013016792.1596553969; _gid=GA1.2.2020073222.1596553969"
        }
        bike_count_data = requests.post(url, body, headers=headers, verify=False)
        # no data available for location on current day
        print(bike_count_data.json())
        if not bike_count_data.json()[:-1]:
            continue
        bike_count_data_entry = bike_count_data.json()[:-1][0]
        # -------------------------------------------------
        # end get bike count data

        # start get public holiday data
        # -------------------------------------------------
        province_public_holidays = []
        geolocator = Nominatim(user_agent="everyonecounts")
        location = geolocator.reverse(str(row['lat']) + "," + str(row['lon']))
        # when city=province, state is not returned
        if 'state' in location.raw['address']:
            province = location.raw['address']['state']
        else:
            province = location.raw['address']['city']
        province_abb = province_abbs[province]
        for date in holidays.DE(years=[start_year], prov=province_abb):
            province_public_holidays.append(str(date))
        # end get public holiday data
        # -------------------------------------------------

        data_set = {'date': str(start_date).split()[0],
                    'bike_count': str(bike_count_data_entry[1]),
                    'name': row['nom'],
                    'lon': row['lon'],
                    'lat': row['lat'],
                    'is_holiday': 1 if str(start_date).split()[0] in province_public_holidays else 0
                    }
        data_sets.append(data_set)
    return pd.DataFrame(data_sets).fillna(0).to_json(orient='records')


def write_data_to_s3(datajson, date):
    client = boto3.client('s3')
    response = client.put_object(
        Bucket='sdd-s3-bucket',
        Body=json.dumps(datajson),
        Key='fahrrad/{}/{}.json'.format(date.strftime('%Y/%m/%d'), str(date).split()[0])
    )
    return response


if __name__ == '__main__':
    yesterday_date = datetime.today() - timedelta(1)
    data_json = get_data(yesterday_date)
    write_data_to_s3(data_json, yesterday_date)
    print(data_json)
