#!/usr/bin/env
# -*- coding: utf-8 -*-
# author: Christos Georgiades
# contact: chr.georgiades@gmail.com
# date: 15-11-2022

import os, glob
import sys
import time
import datetime

import inspect
import subprocess
from ast import literal_eval

from threading import Lock

import atexit

import rospy
import sqlite3

from databasePacketImports import *


################

DEBUG = 0


ros_version=subprocess.check_output(['rosversion', '-d']).decode("utf-8").strip()
print('ros_version:', ros_version)

dbInstance = None
dbInstanceName = None



    
# Performs thread safe database action
def executeQuery(query):
    global dbCursor, dbMutex
    
    dbResult = ""
    
    try:        
        dbMutex.acquire()
        dbResult = dbCursor.execute(query)
        dbMutex.release()
    except Exception as e:
        dbMutex.release()
        print('#==#')
        print('Offending Query:', query)
        print('Exception:', e)
        print('=##=')
        
        
    print(dbResult)


# Performs thread safe database write
def executeQueryWrite(query, values):
    global dbInstance, dbCursor, dbMutex
    
    try:        
        dbMutex.acquire()
        dbCursor.executemany(query, values)
        dbInstance.commit()
        dbMutex.release()
    except Exception as e:
        dbMutex.release()
        print('#==#')
        print('Offending Query:', query)
        print('Values:', values)
        print('Exception:', e)
        print('=##=')
    

# Performs thread safe database action, returns result
def executeQueryFetch(query, rowfactory = None):
    try:
        dbMutex.acquire()
        dbCursor.row_factory = rowfactory
        dbResult = dbCursor.execute(query)
        dbResult = dbCursor.fetchall()
        dbCursor.row_factory = None
        dbMutex.release()
    except Exception as e:
        dbCursor.row_factory = None
        dbMutex.release()
        
        print('#==#')
        print('Offending Query:', query)
        print('row_factory:', rowfactory)
        print('Exception:', e)
        print('=##=')
         
    return dbResult
       

# Given a ros packet returns its type
def getPacketType(packet):
    packetType = str(type(packet)).split('.')[-1].replace('\'>','')
    
    if DEBUG:
        print('packetType: ' + str(packetType))

    return packetType


# Walks through directories until it finds a file under a certain name
def find(name, path):
    #print(path)
    for root, dirs, files in os.walk(path):
        #print(files)
        if name in files:
            return os.path.join(root, name)


# Searches in the expected directories to retrieve the .msg file of a given packet
def findMsgFile(packetName):
    if DEBUG:
        print("findMsgFile")   
        print(packetName)
    
    # Sanitise packet Name
    packetName = packetName.replace("[]", "")
        
    if '/' in packetName:
        packetName = packetName.split('/')[-1]
        
    if ".msg" not in packetName:
        packetName = packetName + ".msg"

    
    if DEBUG:
        print(packetName)
    
    try:
        # Check where the original message archetype might be stored
        packetObject = getattr(sys.modules[__name__], packetName.replace('.msg', ''))
        packetDirectory = inspect.getfile(packetObject)
        
        if DEBUG:
            print('packetObject:', packetObject)
            print('packetDirectory:', packetDirectory)
            
        
        if 'devel' in packetDirectory:
            msgfilePath = find(packetName, os.path.abspath(os.getcwd()))
        else:
            msgfilePath = find(packetName, '/opt/ros/' + ros_version + '/share/')
    except Exception as e:
        if DEBUG:
            print(e)
        
        msgfilePath = find(packetName, '/opt/ros/' + ros_version + '/share/')
        
        if not msgfilePath:
            msgfilePath = find(packetName, os.path.abspath(os.getcwd()))
                       
    if DEBUG:    
        print(msgfilePath)
        
    if not msgfilePath:
        print('Could not find message file: ' + str(packetName))
        
    return msgfilePath


# Serializes packet values to be saved in db
def serializeRosPacketRecursive(packet, depth = -1):    
    if depth == 0:
        return [], []    
    
    packetType = getPacketType(packet)
       
    if 'list' in packetType:
        if DEBUG:           
            print('List Size:', len(packet))
            print('List:', packet)
        
        try:
            packetType = getPacketType(packet[0])
        except Exception as e:
            print('====')
            print('databaseUtils - Line 175 Exception:', e)
            print('Problem Packet:', packet)
            print('packetType:', packet)
            print('depth:', depth)
            print('====')
        

    msgPath = findMsgFile(packetType)
    
    if msgPath is None:
        print('Could not find message type for ' + str(packetType) + '. Exiting...')
        return
       
    if DEBUG:
        print('#=serializeRosPacketRecursive=#')
        print('packet:', packet)
        print('packetType:', packetType)
        print('msgPath:', msgPath)
        
    f = open(msgPath, "r")
    fields = [line for line in f.readlines() if line.strip() if not line.startswith('#')]
    f.close()

    datavalues = []
    datafields = []
    datatypes = []    
    
    for field in fields:
        ogField = field
        if DEBUG:
            print(field.strip())
            
        
        
        if '#' in field:
            field = field.split('#')[0]
            
            if len(field.strip()) == 0:
                if DEBUG:
                    print('Field Length == 0\nContinuing...')
                continue
                                
        if '=' in field:
            if DEBUG:
                print('Constant Field\nContinuing...')
            continue
                                
        if '=' not in field:
            field = field.strip()           
            field = ' '.join(field.split(' ')).split(' ', 1) 
            
        datatype = field[0].strip()
            
        try:
            if DEBUG:
                print('== Field Serialization ==')
                print('Original Field: ', ogField)
                print('Split Field: ', field)
                #print('dir:', dir(packet))
                print('Original Datavalue:', getattr(packet, field[1].strip()))
                print('== Field Serialization END ==')
        except Exception as e:
            print('#== ERROR 220 ==#')
            print('Original Field:', ogField)
#            print('Original Field:', ogField)
#            print('Original Field: ', ogField)
#            print('Original Field: ', ogField)
            print('Split Field:', field)
            #print('dir:', dir(packet))
            print('packet:', packet)  
            print('#== ERROR 220 END ==#')
            
            
        if datatype:
            datafield = field[1].strip()
            
            if type(packet) is list:
                datavalue = []
                for i in range(len(packet)):
                    datavalue.append(getattr(packet[i], datafield))
                
                datavalue = tuple(datavalue)
            else:
                datavalue = getattr(packet, datafield)
                
            
            if DEBUG:
                print('Datavalue:', datavalue)
            
            
            if datatype.strip() and datafield.strip(): 
                if DEBUG:
                    print('=== Before serializeDatafield ===')
                    print(datavalue, datafield, datatype)
                
                
                datavalue, datafield, datatype = serializeDatafield(datavalue, datafield, datatype, depth)
                
                if DEBUG:
                    print('=== After serializeDatafield ===')
                    print(datavalue, datafield, datatype)
                
                if DEBUG:
                    print('=== Before concat to lists ===')
                    print(datavalues, datafields, datatypes)
                
                if type(datavalue) is list:
                    if DEBUG:
                        print('List')
                        print(datavalue, datafield, datatype)
                    
                    datavalues.extend(datavalue)
                    datafields.extend(datafield)
                    datatypes.extend(datatype)
                    
                    if DEBUG:
                        print('Result Datavalues:', datavalues)
                else:
                    if DEBUG:
                        print('Not List')
                    
                    #datavalues.append(sanitiseData(subDatavalues))
                
                    #serializeDatatype()
                    datavalues.append(datavalue)
                    datafields.append(datafield)
                    datatypes.append(datatype)
                    
                if DEBUG:
                    print('=== After concat to lists ===')
                    print(datavalues, datafields, datatypes)
         
                    
    return datavalues, datafields, datatypes


# Deserialize sql values to recreate a ros packet 
def deserializeRosPacketRecursive(sqlvalues, packetType):
    if DEBUG:
        print('Building Ros packet')
    
    packet_class = globals()[packetType]
    
    path = os.path.abspath(os.getcwd())
    if DEBUG  or True:
        print('Current Path:', path)
        print('Looking for:', packetType)
        
    #packetMsg = findMsgFile(packetType + '.msg')
    
        print('Deserialize - Packet Class:', packet_class)
    
    
    dataPackets = []
    
    if packet_class:
        # Build mock ros message, Retrieve datafields and datatypes only one depth down
        _, datafields, datatypes = serializeRosPacketRecursive(packet_class(), depth = 1)      
        
        if DEBUG or True:
            print('\n' + packetType)
            print('datafields\tdatatypes')
            for i in range(len(datafields)):
                print(datafields[i], datatypes[i])
                
            print('')
        
        for entry in sqlvalues:
            packet = packet_class()
            
            if DEBUG:
                print('Entry:', entry)
                       
            
            for i in range(len(datafields)):
                #print(entry[i])
                if '[]' in datatypes[i]:
                    #print(datatypes[i], entry[i])
                    #print(literal_eval(entry[i]))
                    setattr(packet, datafields[i], literal_eval(entry[i]))
                    
                elif isinstance(entry[i], unicode):
                    setattr(packet, datafields[i], entry[i].encode('utf-8'))
                else:
                    setattr(packet, datafields[i], entry[i])
            
            
            dataPackets.append(packet)
            if DEBUG:
                print('Finished building packet ' + str(entry[0]))
                print('=#serializeRosPacketRecursive - END #=')
    
    return dataPackets 
    

def serializeDatafield(datavalue, datafield, datatype, depth = -1):
    if DEBUG:
            print('= serializeDatafield =')
            print(datavalue, datafield, datatype)
        
    # Simple Datatypes
    if not complexDatatype(datatype):                     
        datavalue = sanitiseData(datavalue)
        #datafields.append(datafield)
        #datatypes.append(datatype)
        
        if DEBUG:
            print(datavalue, datafield, datatype)
    
    # Complex Datatypes
    else:
        if DEBUG:
            print('= Complex Start =')
        
        if datavalue is None or depth is None:
            print(datavalue, depth)
        
        subDatavalues, subDatafields, subDatatypes = serializeRosPacketRecursive(datavalue, depth-1)
        
        datavalue = subDatavalues
        datafield = subDatafields
        datatype = subDatatypes
        
        if DEBUG:
            print(subDatavalues, subDatafields, subDatatypes)
            print(datavalue, datafield, datatype)
            print('== Complex End ==')
     
    if DEBUG:
        print(datavalue, datafield, datatype)
        print('= serializeDatafield END =')
        
    return datavalue, datafield, datatype   


# Returns true if ros packet contains other ros packets recursively
def complexDatatype(datatype):
    if DEBUG:
        print("Datatype:", datatype)
    
    if (any(char.isupper() for char in datatype)):
        if DEBUG:
            print("Complex")
        return True
    else:
        if DEBUG:
            print("Simple")
        return False


# Serializes complex/recursive ros packets
def buildRecursiveMessageType(packet, tableName):
    datavalues = []
    
    tableName = getPacketType(packet)
    packetMsg = findMsgFile(tableName)
    
    datavalues, datafields, datatypes = serializeRosPacketRecursive(packet, depth=-1) 
      
    if DEBUG or True:
        print('\n' + tableName)
        print('datatypes\tdatafields')
        for i in range(len(datafields)):
            print(str(datatypes[i]) + '\t' + str(datafields[i]))
            
        print('')      
        
    
    for i in range(len(datafields)):
        if DEBUG:
            print("PACKET TYPE:", type(packet))
            print("PACKET:", packet)
            print(datafields)
        
        if type(packet) is list:
            if DEBUG:
                print(type(packet[0]))
                print(packet[0])

            for j in range(len(packet)):
                data = buildRecursiveMessageType(packet, getPacketType(packet))
                
                if DEBUG:
                    print(data)
        else:
            fieldValue = getattr(packet, datafields[i])
            if DEBUG:
                print('datatype ' + str(i) + ': ', datatypes[i])
                print('type:', type(fieldValue))
                print('field value ' + str(i) + ': ', fieldValue)

            if type(fieldValue) is list:
                unpackedFieldValue = []
                
                for value in fieldValue:
                    if DEBUG:
                        print(type(value))
                        print(value)

                    unpackedFieldValue.append(buildRecursiveMessageType(value, getPacketType(packet)))
                
                if DEBUG:
                    print('UnpackedFieldValue:', unpackedFieldValue)
                 
                valueTuple = () 
                if len(unpackedFieldValue) > 0:
                    for k in range(len(unpackedFieldValue[0])):

                        for l in range(len(unpackedFieldValue)):
                            valueTuple = valueTuple + (unpackedFieldValue[l][k],)
    
                datavalues.append(str(valueTuple))
                
            else:
                if complexDatatype(datatypes[i]):
                    dtType = datatypes[i]
                    if DEBUG:
                        print('Datatype ' + str(i) + ': ' + str(dtType) + ' is complex')
                    if '[]' in dtType:
                        if DEBUG:
                            print('data is an array')
                        dtType = dtType.replace('[]', '')


                    datavalues.extend(buildRecursiveMessageType(fieldValue, dtType))

                else:
                    #sanitise
                    datavalues.append(sanitiseData(fieldValue))
    
    if DEBUG:
        print('Datavalues:')
        for data in datavalues:
            print(data)

        print(datavalues)
        
    return datavalues
 
    
# Sanitises data
def sanitiseData(fieldValue):
    if DEBUG:
        print('\n=# Sanitising Data #=')
        print('fieldValue:', fieldValue)
        print('fieldValue Type:', type(fieldValue))
    
    
    if 'genpy.rostime.Time' in str(type(fieldValue)): #is rospy.rostime.Time:
        fieldValue = int(str(fieldValue.secs) + str(fieldValue.nsecs))
        
    # Lists are not saveable as sql
    if type(fieldValue) is list:
        fieldValue = tuple(fieldValue)
        
    if type(fieldValue) is tuple:
        fieldValue = str(fieldValue)
        
    if DEBUG:    
        print('fieldValue Clean:', fieldValue)
        print('=# Sanitising Data - END #=')

    return fieldValue

topicTypeDict = {}
topicTypeDictMutex = Lock()
def getRostopicType(topic):
    global topicTypeDict, topicTypeDictMutex
    
    topicType = None
    
    topicTypeDictMutex.acquire()
    if topic in topicTypeDict:
        topicType = topicTypeDict[topic] 
    topicTypeDictMutex.release()
    
    if topicType is None:
        topicInfo = subprocess.check_output(['rostopic', 'info', topic])  
        
        topicType = topicInfo.split("Publishers:")[0]
        topicType = topicType.replace("Type: ", "").strip()
        
        topicTypeDictMutex.acquire()
        topicTypeDict[topic] = topicType
        topicTypeDictMutex.release()
        
        
    if DEBUG:
        print('getRostopicInfo - Info:\n' + str(topicType))
    
    return topicType


# Creates directory where database instances will be saved
def createDBDirectory():
    global dbDir
    
    # Create DB directory
    dbDir = 'flightlogs'
    if not os.path.isdir(dbDir):
        os.mkdir(dbDir) 


def createDBInstance(flightlog_path):
    global dbInstance, dbCursor, dbMutex, dbPath
    
    dbInstanceName = flightlog_path.split("/")[-2]

    # Create DB instance
    dbPath = flightlog_path + dbInstanceName + '.db' #+ '-' + datetime.datetime.now().strftime("%Y-%m-%d-%H:%M") + '.db'
    print('Database Handler - dbPath:', dbPath)
    dbInstance = sqlite3.connect(dbPath, check_same_thread=False)
    dbCursor = dbInstance.cursor()
    dbMutex = Lock()


# Function run at exit that removes .db-journal lock-files
def clearDBLock():
    global dbDir
    
    for lf in glob.glob(dbDir + '/*.db-journal'):
        print('Removed Lock: ' + str(lf))
        os.remove(lf)
        
        
def Init(dronename):
    global dbInstanceName, dbStartTime
    
    dbInstanceName = dronename.replace('/', '')
    
    
    
    dbStartTime = datetime.datetime.now()
    rospy.init_node(dronename.replace('/', '') + '_db')

    flightlog_path_pckt = rospy.wait_for_message(dronename + '/FlightLogDirectory', String)
    flightlog_path = flightlog_path_pckt.data
    print('Database Handler - flightlog_path:', flightlog_path)
    #Database Handler - flightlog_path: /home/jetson/Documents/aiders_osdk/flightlogs/matrice300_c501-2023-05-08-T12-08-31/

    createDBDirectory()
    createDBInstance(flightlog_path)
    
    atexit.register(clearDBLock)