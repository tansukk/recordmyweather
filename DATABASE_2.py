import mysql.connector
from mysql.connector import connection

import requests
import requests, json 
import numpy as np
import time
import datetime

#CITIES TO BE INCLUDED IN THE RESEARCH
#BRUSSELS, BE / NEW YORK, US / CALIFORNIA, US / MILAN, IT / ROME, IT / BERLIN, DE / KÖLN, DE / SYDNEY, AU 
# / MOSCOW, RU / AMSTERDAM, NL / BARCELONA, ES / RIO DE JANEIRO, BR / BUENOS AIRES, AR / LONDON, GB 
# / BEIJING, CN / TOKYO, JP / OSLO, NO / BANGKOK, TH / ANKARA, TR / ISTANBUL, TR

cities_list = ["BRUSSELS, BE", "NEW YORK, US", "CALIFORNIA, US", "MILAN, IT", "ROME, IT", "BERLIN, DE", "KÖLN, DE", "SYDNEY, AU", "MOSCOW, RU", 
"AMSTERDAM, NL", "BARCELONA, ES", "RIO DE JANEIRO, BR", "BUENOS AIRES, AR", "LONDON, GB", "BEIJING, CN", "TOKYO, JP", "OSLO, NO", 
"BANGKOK, TH", "ANKARA, TR", "ISTANBUL, TR"]

mydb = mysql.connector.connect(
        user="sql11431944", 
        password="Jc3Su6QgWx",
        host="sql11.freesqldatabase.com",
        database="sql11431944"
        
    )


def update(city):
    global temperature, feels_like, humidity, wind_speed, wind_degree, pressure, temp_difference, row_id, row_number, city_name, main_explanation, description
    api_key = "bf640b2a35c9905fe66d509cde851a4c"
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    city_name = city
    complete_url = base_url + "appid=" + api_key + "&q=" + city_name
    response = requests.get(complete_url)
    x = response.json()
    #print(x)
    #time.sleep(2)
    if x["cod"] != "404": 

    # store the value of "main" key in variable y, z and w
        y = x["main"]
        z = x["wind"]
        w = x["weather"]
        #print(w)
    # store the corresponding values to the variables 
        
        temperature = y["temp"]
        temperature = temperature-273.15 #Check the conversion formula!!
        feels_like = y["feels_like"] 
        feels_like = feels_like-273.15
        temp_difference = feels_like-temperature
        humidity = y["humidity"] 
        pressure = y["pressure"]
        wind_speed = z["speed"]
        wind_degree = z["deg"]
        main_explanation = w[0]["main"]
        description = w[0]["description"]

        #print("Measured weather values in ",city_name, " are :")
        #print("Temperature : ", temperature, "Real feel : ", feels_like, "Humidity : ", humidity)
        #print("Wind speed : ", wind_speed, "Wind degree : ",  wind_degree, "Pressure : ",  pressure)
        #print("Real and Real Feel Temperature Difference : ", temp_difference)
        #print("--------------------------------------------------------------------------------------")
        #return city_name, temperature, feels_like, humidity, wind_speed, wind_degree, pressure, temp_difference, main_explanation, description
    else: 
        print(" City Not Found ")
    


#-------------------------------------INSERTING

row_number = 0
go_on_var = 1
while go_on_var ==1:
    
    time_log = time.time()
    print(time_log)
    timestamp = datetime.datetime.fromtimestamp(time_log)
    #print(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
    for city in cities_list:
        update(city)
        row_id = "RID" + str(row_number)
        row_number = row_number + 1
        mycursor = mydb.cursor()
        sql = "INSERT INTO weather_measurements (timestamp_db, row_id_db, city_db, temp_db, real_feel_db, difference_db, pressure_db, humidity_db, windspeed_db, winddegree_db, mexplanation_db, description_db) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (time_log, row_id, city_name, temperature, feels_like, humidity, wind_speed, wind_degree, pressure, temp_difference, main_explanation, description)
        mycursor.execute(sql, val)
        mydb.commit()
        #print(mycursor.rowcount, "record inserted.", city_name, row_id)
        
    time.sleep(5)

#--------------------------------------------------------------
#CLOSING THE CURSORS AND DATABASE CONNECTION
mycursor.close()
mydb.close()
