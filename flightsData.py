import requests
import pandas as pd
import schedule
import time
import os
import datetime

os.chdir('C:\\Users\\ggarre\\Dropbox (ASU)\\PC\\Downloads\\flightsData')

headers = {
    "X-RapidAPI-Host": "adsbexchange-com1.p.rapidapi.com",
    "X-RapidAPI-Key": "1d0d3ab38bmsh74769b1c33da628p120079jsn8622ee9774f1"
}
resFrames = []

def populateDataFrames(df, popLimit, limit):
    for _, data in df.iloc[:limit].iterrows():
        if data["Population"] > popLimit:
            if "State" in data.keys():
                getLatLonAPI = "https://api.geoapify.com/v1/geocode/search?city={}&state={}&lang=en&limit=1&type=city&format=json&apiKey=cb1ee27116cf4df3b1d942c593957c21".format(
					data["Name"].replace(" ", "%20"), data["State"])
            elif "Country" in data.keys():
                getLatLonAPI = "https://api.geoapify.com/v1/geocode/search?city={}&country={}&lang=en&limit=1&type=city&format=json&apiKey=cb1ee27116cf4df3b1d942c593957c21".format(
					data["Name"].replace(" ", "%20"), data["Country"].replace(" ", "%20"))
            response = requests.request("GET", getLatLonAPI)
            if response and response.json()["results"]:
                response = response.json()["results"][0]
                lon = response["lon"]
                lat = response["lat"]
            url = "https://adsbexchange-com1.p.rapidapi.com/json/lat/{}/lon/{}/dist/250/".format(
                lat, lon)
            response = requests.request("GET", url, headers=headers)
            if response and response.json()["ac"]:
                tempDF = pd.json_normalize(response.json(), 'ac')
                resFrames.append(tempDF)
    print("populateDataFrames -- 1 file processed")

def scheduledJob():
    print("Started Job!!!")
    df = pd.read_csv('200uscities.csv')
    populateDataFrames(df, 400000, 200)
    df = pd.read_csv('World City Populations 2022.csv')
    populateDataFrames(df, 1000000, 50)
    df = pd.read_csv('200uscities.csv')
    populateDataFrames(df, 200000, 200)
    result = pd.concat(resFrames, axis=0, ignore_index=True)
    result.drop_duplicates()
    result.to_csv('results_{}.csv'.format(datetime.date.today().strftime("%B%d_%Y")))

schedule.every().monday.at("13:30").do(scheduledJob)

while True:
    schedule.run_pending()
    time.sleep(600)