from unicodedata import name
import paho.mqtt.client as mqtt
import json 
import sqlite3
import time
import logging
import sys
import datetime 


DB_Name = "solar_panel_sep05.db"


class DatabaseManager():
    def __init__(self):
        self.conn = sqlite3.connect(DB_Name)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    def add_del_update_db_record(self, sql_query, args=()):
        self.cur.execute(sql_query, args)
        self.conn.commit()
        return
    
    def __del__(self):
        self.cur.close()
        self.conn.close()

###################################################

def mbox1_Handler(Topic, jsonData):
    #Parse Data 
    topic1 = Topic.split('/')
    box = topic1[2]
    print("mbox1 topic: -",topic1)
    print("mbox1 extracted topic: -" , box)
    json_Dict = json.loads(jsonData)
    average = json_Dict['avg']
    minimum = json_Dict['min']
    maximum = json_Dict['max']
    maxpos_x = json_Dict['maxpos']['x']
    maxpos_y = json_Dict['maxpos']['y']
    minpos_x = json_Dict['minpos']['x']
    minpos_y = json_Dict['minpos']['y']
    unit = json_Dict['unit']

    #####Define Threshold Limits#####
    threshold = 30
    if(json_Dict['max'] > threshold):
        alert = "high"
    else:
        alert = "low"

    alert_value = alert

    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into master_table (avg, min, max, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert) values (?,?,?,?,?,?,?,?,?,?)",[average, minimum, maximum, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert_value])
    del dbObj
    print("Inserted mbox1 Data into Master Table")
    print("")
    time.sleep(5)

# Function to save to DB Table
def mbox2_Handler(Topic, jsonData):
    #Parse Data 
    topic2 = Topic.split('/')
    box = topic2[2]
    print("mbox2 topic: -",topic2)
    print("mbox2 extracted topic: -" , box)
    json_Dict = json.loads(jsonData)
    average = json_Dict['avg']
    minimum = json_Dict['min']
    maximum = json_Dict['max']
    maxpos_x = json_Dict['maxpos']['x']
    maxpos_y = json_Dict['maxpos']['y']
    minpos_x = json_Dict['minpos']['x']
    minpos_y = json_Dict['minpos']['y']
    unit = json_Dict['unit']

    #####Define Threshold Limits#####
    threshold = 30
    if(json_Dict['max'] > threshold):
        alert = "high"
    else:
        alert = "low"

    alert_value = alert

    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into master_table (avg, min, max, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert) values (?,?,?,?,?,?,?,?,?,?)",[average, minimum, maximum, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert_value])
    del dbObj
    print("Inserted mbox2 Data into Master Table")
    print("")
    time.sleep(5)

def mbox3_Handler(Topic, jsonData):
    #Parse Data 
    topic3 = Topic.split('/')
    box = topic3[2]
    print("mbox1 topic: -",topic3)
    print("mbox1 extracted topic: -" , box)
    json_Dict = json.loads(jsonData)
    average = json_Dict['avg']
    minimum = json_Dict['min']
    maximum = json_Dict['max']
    maxpos_x = json_Dict['maxpos']['x']
    maxpos_y = json_Dict['maxpos']['y']
    minpos_x = json_Dict['minpos']['x']
    minpos_y = json_Dict['minpos']['y']
    unit = json_Dict['unit']

    #####Define Threshold Limits#####
    threshold = 30
    if(json_Dict["max"] > threshold):
        alert = "high"
    else:
        alert = "low"

    alert_value = alert
    
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into master_table (avg, min, max, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert) values (?,?,?,?,?,?,?,?,?,?)",[average, minimum, maximum, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert_value])
    del dbObj
    print("Inserted mbox3 Data into Master Table")
    print("")
    time.sleep(5)


def mbox4_Handler(Topic, jsonData):
    #Parse Data 
    topic4 = Topic.split('/')
    box = topic4[2]
    print("mbox1 topic: -",topic4)
    print("mbox1 extracted topic: -" , box)
    json_Dict = json.loads(jsonData)
    average = json_Dict['avg']
    minimum = json_Dict['min']
    maximum = json_Dict['max']
    maxpos_x = json_Dict['maxpos']['x']
    maxpos_y = json_Dict['maxpos']['y']
    minpos_x = json_Dict['minpos']['x']
    minpos_y = json_Dict['minpos']['y']
    unit = json_Dict['unit']

    #####Define Threshold Limits#####
    threshold = 30
    if(json_Dict["max"] > threshold):
        alert = "high"
    else:
        alert = "low"

    alert_value = alert
    
    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into master_table (avg, min, max, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert) values (?,?,?,?,?,?,?,?,?,?)",[average, minimum, maximum, maxpos_x, maxpos_y, minpos_x, minpos_y, unit, box, alert_value])
    del dbObj
    print("Inserted mbox4 Data into Master Table")
    print("")
    time.sleep(5)

def tempsensor_Handler(jsonData):
    #Parse Data 
    json_Dict = json.loads(jsonData)
    sensor = json_Dict['sensor']
    name = json_Dict['name']
    unit = json_Dict['unit']

    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into tempsensor (sensor, name, unit) values (?,?,?)",[sensor, name, unit])
    del dbObj
    print("Inserted mbox4 Data into Database")
    print("")
    time.sleep(5)


#Solar Sensor data -> Bus Voltage, Shunt Voltage, current, line-voltage, p

def solar_sensor_Handler(jsonData):
    #Parse Data 
    received_data_payload = jsonData.decode()    
    received_data_payload = received_data_payload.replace("'",'"')
    
    json_Dict = json.loads(received_data_payload)
    print("sensor data",json_Dict)
    bus_voltage = json_Dict['bv']
    shunt_voltage = json_Dict['sv']
    current = json_Dict['i']
    line_voltage = json_Dict['lv']
    power = json_Dict['p']

    dbObj = DatabaseManager()
    dbObj.add_del_update_db_record("insert into solar_sensor (bv, sv, i, lv, p) values (?,?,?,?,?)",[bus_voltage,shunt_voltage, current, line_voltage, power])
    del dbObj
    print("Inserted solar_sensor Data into Database")
    print("")
    time.sleep(5)

    
# Master Function to Select DB Function based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
    if Topic == "FLIR/ec501-106AAB/mbox1":
        mbox1_Handler(Topic, jsonData)
    elif Topic == "FLIR/ec501-106AAB/mbox2":
        mbox2_Handler(Topic, jsonData)
    elif Topic == "FLIR/ec501-106AAB/mbox3":
        mbox3_Handler(Topic, jsonData)
    elif Topic == "FLIR/ec501-106AAB/mbox4":
        mbox4_Handler(Topic, jsonData)
    elif Topic == "solar/p01/metrics":
        solar_sensor_Handler(jsonData)
    
