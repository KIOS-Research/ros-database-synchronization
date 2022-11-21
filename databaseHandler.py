#!/usr/bin/env pythonfind . 
# -*- coding: utf-8 -*-
# author: Christos Georgiades
# contact: chr.georgiades@gmail.com
# date: 15-11-2022

import os
import sys
import datetime

import inspect
from ast import literal_eval

import atexit

from threading import Thread, Lock

import rospy
import sqlite3

# .Msg Imports #
from std_msgs.msg import String, Bool, UInt32
from sensor_msgs.msg import BatteryState

from geometry_msgs.msg import Vector3Stamped, QuaternionStamped
from multimaster_msgs_fkie.msg import LinkStatesStamped

#from dji_sdk.msg import BatteryState
from dji_sdk.msg import EscData, WindData
from dji_sdk.msg import FlightAnomaly
from dji_sdk.msg import ComponentList

from kios.msg import MissionDji, trisonica_msg
from kios.msg import Telemetry, TerminalHardware, DroneHardware, Positioning, DroneMovement, DatabasePacketRequest
from kios.msg import BuildMap
################

DEBUG = 0

# Create DB directory
dbDir = 'database'
if not os.path.isdir(dbDir):
    os.mkdir(dbDir) 

# Create DB instance
dbInstance = dbDir + '/drone' + datetime.datetime.now().strftime("%Y-%m-%d-%H:%M") + '.db'
db = sqlite3.connect(dbInstance, check_same_thread=False)
dbCursor = db.cursor()
dbMutex = Lock()


def getPacketType(packet):
    packetType = str(type(packet)).split('.')[-1].replace('\'>','')
    
    if DEBUG:
        print('packetType: ' + str(packetType))

    return packetType


def find(name, path):
    for root, dirs, files in os.walk(path):
        #print(files)
        if name in files:
            return os.path.join(root, name)


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
            msgfilePath = find(packetName, "/opt/ros/melodic/share/")
    except Exception as e:
        if DEBUG:
            print(e)
        
        msgfilePath = find(packetName, "/opt/ros/melodic/share/")
        
        if not msgfilePath:
            msgfilePath = find(packetName, os.path.abspath(os.getcwd()))
                       
    if DEBUG:    
        print(msgfilePath)
        
    if not msgfilePath:
        print('Could not find message file: ' + str(packetName))
        
    return msgfilePath


# Print DB metrics at exit()
entryCount = {}
#requestCount = {}
def exit_handler():
    global dbStartTime
    
    tableNames = getTableNames()
    dbCompleteness = 0
    for table in tableNames:
        query = 'SELECT COUNT(*) FROM ' + table
        
        dbMutex.acquire()
        dbCursor.row_factory = lambda cursor, row: row[0]
        dbResult = dbCursor.execute(query)
        dbResult = dbCursor.fetchall()
        dbCursor.row_factory = None
        dbMutex.release()
        
        completeness = dbResult[0] * 1.0 / entryCount[table] * 100.0
        print(table + '\t' + str(dbResult[0]) + '/' + str(entryCount[table]) + '\t' + str(completeness) + '%')
        dbCompleteness = dbCompleteness + completeness
        
    print('Database Completeness\t' + str(dbCompleteness/len(tableNames)) + '%\n')
    dbEndTime = datetime.datetime.now()
    dbRuntime = dbEndTime - dbStartTime                         
    duration_in_s = dbRuntime.total_seconds()   
    
    print('DB runtime:\t' + str(duration_in_s) + '[s]')


def savePacketCB(packet):
    global databaseActive
    global entryCount
    
    if databaseActive:
        if DEBUG:
            print(str(type(packet)))
    
        tableNames = getTableNames()   
        packetName = getPacketType(packet)
        
        if packetName not in entryCount:
            entryCount[packetName] = 0
        
        entryCount[packetName] = entryCount[packetName] + 1
            
        if packetName not in tableNames:
            createTable(packet)
            
        saveData(packet, packetName)    
       
    
def createTable(packet):    
#    packetName = tableName
#    packetType = packetName + '.msg'
    packetType = getPacketType(packet)
    
    
    if DEBUG:
        print("packetType:", packetType)
    
    # Find original .msg file
    # packetMsg = findMsgFile(packetType) 
    
    #if packetMsg:
    # Recurse through its contents counting how many fields are needed
    datavalues, datafields, datatypes = serializeRosPacketRecursive(packet)
    
        
    query = "CREATE TABLE " + packetType + " (" + ",".join(datafields) + ")"
    if DEBUG:
        print(query)
        
    try:
        dbMutex.acquire()
        dbResult = dbCursor.execute(query)
        dbMutex.release()
    except Exception as e:
        dbMutex.release()
        if DEBUG:
            print(e)
            print(query)
            print(packet)

    if DEBUG:        
        print(packetType + ' table created')


def getTableNames():
    global dbCursor

    dbMutex.acquire()
    dbCursor.row_factory = lambda cursor, row: row[0]
    dbResult = dbCursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    dbResult = dbCursor.fetchall()
    dbCursor.row_factory = None
    dbMutex.release()
    
    return dbResult


def saveData(packet, tableName):
    datavalues = []
    
    if DEBUG:
        print('Converting ros packet to sql entry')
        
    datavalues, datafields, datatypes = serializeRosPacketRecursive(packet, depth=-1)
    
    for i in range(len(datavalues)):
        if type(datavalues[i]) is list:
            datavalues[i] = str(tuple(datavalues[i]))
            
    
    if DEBUG:
        print('\n' + tableName)
        print('datafields\tdatatypes\tdatavalues')
        for i in range(len(datavalues)):
            print(datafields[i], datatypes[i], datavalues[i])
            
        print('')
               
    #datavalues = buildRecursiveMessageType(packet, tableName)

    linecount = len(datavalues)
    if DEBUG:
        print('linecount', linecount)
        print('datafields', datavalues)
        print('datavalues', datavalues)
        
    query = 'INSERT INTO ' + tableName +  ' VALUES('
    for i in range(linecount - 1):
        query += '?, '
    
    query += '?)'
    
    if DEBUG:
        print('tableName:', tableName)
        print('query:', query)
        print('tuple:', [tuple(datavalues)])

    try:
        dbMutex.acquire()
        dbCursor.executemany(query, [tuple(datavalues)])
        db.commit()
        dbMutex.release()
    except Exception as e:
        dbMutex.release()
        
        sys.stderr.write(str(e) + "\n")
        sys.stderr.write("SQL tuple: " + str(tuple(datavalues)) + "\n")
        sys.stderr.write("Packet Content: " + str(packet) + "\n")
        sys.stderr.write("Table Name: " + str(tableName) + "\n")
   
    if DEBUG:        
        print('Saving Data Done\t' + tableName)
    if hasattr(packet, 'seq') and hasattr(packet, 'uid'):
        if packet.uid == boardId:
            propagateData(packet, tableName)
            #checkDataCompletion(packet, tableName)
        #else:
        checkDataCompletion(packet, tableName)


def serializeRosPacketRecursive(packet, depth = -1):    
    if depth is 0:
        return [], []    
    
    packetType = getPacketType(packet)
    
    if 'list' in packetType:
        print('List Size:', len(packet))
        print('List:', packet)
        packetType = getPacketType(packet[0])

    msgPath = findMsgFile(packetType)
       
    if DEBUG:
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
        if DEBUG:
            print(field.strip())
        
        if '#' in field:
            field = field.split('#')[0]
                                
        if '=' not in field:
            field = field.strip()
            
            field = ' '.join(field.split(' ')).split(' ', 1) 
            
            datatype = field[0].strip()
            
            if DEBUG:
                print('Original Field: ', field)
                print('Split Field: ', field)
                    
            
            
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
                    # Simple Datatypes
                    if not complexDatatype(datatype):                     
                        datavalues.append(sanitiseData(datavalue))                    
                        datafields.append(datafield)
                        datatypes.append(datatype)
                        
                        if DEBUG:
                            print(datatype, datafield, datavalue )
                    # Complex Datatypes
                    else:
                        if DEBUG:
                            print('= Complex Start =')
                            
                        subDatavalues, dtf, dtp = serializeRosPacketRecursive(datavalue, depth-1)
                        
                        if DEBUG:
                            print(dtf, dtp, subDatavalues)
                            print('== Complex End ==')
                            
                        datafields.extend(dtf)
                        datatypes.extend(dtp)
                        
                        if type(subDatavalues) is list:
                            print('List')
                            print('Original Datavalues:', datavalues)
                            print('SubDatavalues:', subDatavalues)
                            
                            
                            datavalues.extend(sanitiseData(subDatavalues))
                            
                            print('Result Datavalues:', datavalues)
                        else:
                            print('Not List')
                            datavalues.append(sanitiseData(subDatavalues))

    return datavalues, datafields, datatypes


def deserializeRosPacketRecursive(sqlvalues, packetType): 
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
            if DEBUG  or True:
                print('Finished building packet ' + str(entry[0]))
    
    return dataPackets 
    

def performDatabaseAction(query):
    if DEBUG:
        print(query)
    
    performDatabaseAction(query)
        
    dbMutex.acquire()
    dbResult = dbCursor.execute(query)
    dbMutex.release()


def checkIfEntryExists(packet, packetName): 
    if packet.uid == boardId:
        return False
    
    query = "SELECT * FROM " + str(packetName) + " WHERE uid=\'" + packet.uid + "\' AND seq=" + str(packet.seq)
    
    if DEBUG:
        print(query)
    
    
    dbMutex.acquire()
    dbCursor.row_factory = lambda cursor, row: row[0]
    dbResult = dbCursor.execute(query)
    dbResult = dbCursor.fetchall()
    dbCursor.row_factory = None
    dbMutex.release()
    
    if DEBUG:
        print("Retrieved results: " + str(len(dbResult)))
    
    if len(dbResult) > 0:
        return True
    else:
        return False 


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
 
    
def sanitiseData(fieldValue):
    if DEBUG:
        print('\nSanitising Data')
        print('fieldValue:', fieldValue)
        print('fieldValue Type:', type(fieldValue))
    
    
    if 'genpy.rostime.Time' in str(type(fieldValue)): #is rospy.rostime.Time:
        fieldValue = int(str(fieldValue.secs) + str(fieldValue.nsecs))
        
    
#    if type(fieldValue) is list:
#        #fieldValue = tuple(fieldValue)
#        pass
    if type(fieldValue) is tuple:
        fieldValue = str(fieldValue)
        
    if DEBUG:    
        print('fieldValue Clean:', fieldValue)

    return fieldValue


databaseActive = True           
def databaseActiveCB(dbState):
    global databaseActive
    
    print('databaseActiveCB', dbState)
    
    if dbState.data:
        databaseActive = True
    else:
        databaseActive = False      
        
    print('Database State - ' + str(databaseActive))


requestTableLock = {}        
def dataRequestCB(dataRequest):
    global requestTableLock
    
    print('Received data request:', dataRequest)
    
    senderTable = (str(dataRequest.sender) + '/' + str(dataRequest.table))
    print('Sender Table:', senderTable)
    if senderTable in requestTableLock:
        print('senderTable Value:' + str(requestTableLock[senderTable]))
    
    if senderTable not in requestTableLock:
        requestTableLock[senderTable] = True
    elif requestTableLock[senderTable]:
        print('Data request already in progress for table ' + dataRequest.table)
        return
    elif not requestTableLock[senderTable]:
        requestTableLock[senderTable] = True
    
    
    if DEBUG or True:
        print('\nGot a request for data from ' + str(dataRequest.sender) + '\nasking for entries [' + str(dataRequest.from_seq) + ':' + str(dataRequest.to_seq) + '] from table ' + str(dataRequest.table))   
    
    query = "SELECT * FROM " + str(dataRequest.table) + " WHERE uid=\'" + str(boardId) + "\' AND seq BETWEEN " + str(dataRequest.from_seq) + " AND " + str(dataRequest.to_seq)
    
    if DEBUG or True:
        print('Data Retrieval Query:\n\t' + str(query))
    
    try:
        dbMutex.acquire()
        selectResult = dbCursor.execute(query)
        resultList = selectResult.fetchall()
        dbMutex.release()
    except Exception as e:
        print('dataRequestCB - Exception:', e)
        return
    
    #print('Result List:\n\t', resultList)
    
    #for entry in resultList:
    #   print(entry)
    
    #topic = '/matrice300_' + dataRequest.sender + '/' + dataRequest.table
    topic = dataRequest.sender + '/' + dataRequest.table
    if DEBUG or True:
        print ('DataRequest - Publishing to: ' + topic)
        
    datarequestPub = rospy.Publisher(topic, getattr(sys.modules[__name__], str(dataRequest.table)), queue_size=10)
    
    packet_class = globals()[dataRequest.table]
    
    path = os.path.abspath(os.getcwd())
    if DEBUG:
        print('Current Path:', path)
        print('Looking for:', dataRequest.table)
    packetMsg = findMsgFile(dataRequest.table + '.msg') #find(dataRequest.table + '.msg', path)
    
    if packetMsg:
        #datafields, datatypes = readMsgFile(packetMsg)
        
        if len(resultList) > 0:
            packetList = deserializeRosPacketRecursive(resultList, dataRequest.table)
            
            for packet in packetList:
                datarequestPub.publish(packet)
                
                if DEBUG or True:
                    print('datarequestcb - Just published packet ' + str(packet))
        
        
        
#        for entry in resultList:
#            packet = packet_class()
#            
#            for i in range(len(datafields)):
#                #print(entry[i])
#                if '[]' in datatypes[i]:
#                    #print(datatypes[i], entry[i])
#                    #print(literal_eval(entry[i]))
#                    setattr(packet, datafields[i], literal_eval(entry[i]))
#                    
#                elif isinstance(entry[i], unicode):
#                    setattr(packet, datafields[i], entry[i].encode('utf-8'))
#                else:
#                    setattr(packet, datafields[i], entry[i])
#            
#            datarequestPub.publish(packet)
#            if DEBUG:
#                print('Just published packet ' + str(entry[0]))
        
    
    #print('Entry List END')  
    requestTableLock[senderTable] = False
    
    if DEBUG:
        print('Data Request Finished')
    

def propagateData(packet, tableName):
    if DEBUG:
        print('Propagating data\t' + tableName)
        print(tableName)
        print(packet)
    
    topics = rospy.get_published_topics()

    for topic in topics:
        if tableName in topic[0]:
            if not dronename in topic[0]: 
                if DEBUG:
                    print('Propagating to:\n\t' + topic[0] + '\t' + tableName)
                tempPub = rospy.Publisher(topic[0], getattr(sys.modules[__name__], tableName), queue_size=1)
                tempPub.publish(packet)


unfilledDataRequests = {}
dataRequestCount = {}
dataRequestTimestamp = {}
def checkDataCompletion(packet, tableName):
    global unfilledDataRequests, dataRequestCount, dataRequestTimestamp

    # Check if the incoming packet fulfills a data request
    uidTable = packet.uid + '_' + tableName
    if uidTable in unfilledDataRequests:       
        if DEBUG: 
            print(unfilledDataRequests)     
        
        if packet.seq >= unfilledDataRequests[uidTable].from_seq and packet.seq <= unfilledDataRequests[uidTable].to_seq:
            dataRequestCount[uidTable] = dataRequestCount[uidTable] + 1
            
            requestSize = unfilledDataRequests[uidTable].to_seq - unfilledDataRequests[uidTable].from_seq
            if dataRequestCount[uidTable] >= requestSize:
                print('Request Fulfilled:', unfilledDataRequests[uidTable])
                del unfilledDataRequests[uidTable]
                del dataRequestCount[uidTable]
                del dataRequestTimestamp[uidTable]
                print('Data Request fulfilled - Removed entry')
                
            
            
            #print(unfilledDataRequests)
            #del unfilledDataRequests[uidTable]
            #print(unfilledDataRequests)
            
        else:  
            print('===================================')
            print('Data Request unfulfilled - Awaiting')
            
            print('unfilledDataRequests[uidTable]:', unfilledDataRequests[uidTable])
            print('unfilledDataRequests[uidTable]:', dataRequestCount[uidTable])
            print('dataRequestTimestamp[uidTable]:', dataRequestTimestamp[uidTable])
            print('===================================')
            
            return
    
    # If a data request is not in process, identify any possible gaps in data
    else:
        requestList = buildRequestList(packet, tableName) 
        
        print('Request List:', requestList)
        
        if len(requestList) > 0:
            if DEBUG:
                print('Packet Request List for table ' + tableName + '\n\t' + str(requestList))
                
            unfilledDataRequests[uidTable] = 0
        
            for packetRequest in requestList:
                sendMissingDataRequest(packet, tableName, packetRequest)
                
#                packetRequest.timestamp = datetime.datetime.now()
#                packetRequest.count = 0
                
#                setattr(packetRequest, 'timestamp', datetime.datetime.now())
#                setattr(packetRequest, 'count', 0)
                
                unfilledDataRequests[uidTable] = packetRequest
                dataRequestCount[uidTable] = 0
                dataRequestTimestamp[uidTable] = datetime.datetime.now()

                
                print('Packet Request before being saved:', packetRequest)
                


def buildRequestList(packet, tableName):
    query = "SELECT seq, uid FROM " + tableName + " WHERE uid=\'" + packet.uid + "\' ORDER BY seq DESC"
    if DEBUG:
        print('Check data completion called')
        print('Select Query:\n\t' + str(query))
    
    
    # Retrieve seq numbers from DB
    dbMutex.acquire()
    dbCursor.row_factory = lambda cursor, row: row[0]
    selectResult = dbCursor.execute(query)
    seqList = selectResult.fetchall()
    dbCursor.row_factory = None
    dbMutex.release()
    
    print ('seqlist for table ' + str(tableName) + '/' + str(packet.uid) + ' :', seqList)
    
    if DEBUG:
        print('seqList built')
        print(seqList)
    
    # Find gaps in the seq numbers
    # Create a list of the needed packets to fulfill the gaps
    requestList = []   
    iMax = 0
    iMin = 1
    if len(seqList) == 1:
        if seqList[0] > 1:
            requestList.append(createPacketRequest(dronename, tableName, 0, seqList[0] - 1))
    else:
        while iMin < len(seqList):
            if seqList[iMax] - seqList[iMin] > 1:
                requestList.append(createPacketRequest(dronename, tableName, seqList[iMin] + 1, seqList[iMax] - 1))              
            elif iMin == len(seqList) - 1 and seqList[iMin] > 1:
                requestList.append(createPacketRequest(dronename, tableName, 0, seqList[iMax] - 1))

            iMax += 1
            iMin += 1 
            
    return requestList


def createPacketRequest(dronename, table, from_seq, to_seq):
    packetReq = DatabasePacketRequest()
    packetReq.sender = dronename
    packetReq.table = table
    
    packetReq.from_seq = from_seq
    packetReq.to_seq = to_seq
    
    return packetReq
    

def sendMissingDataRequest(packet, tableName, requestMsg):   
    topicName = '/matrice300_' + packet.uid + '/DataRequest' 
    
    if DEBUG:
        print('Sending data request:\n\t' + topicName)
    
    dataReqPub = rospy.Publisher(topicName, DatabasePacketRequest, queue_size=1, latch=True)    
    dataReqPub.publish(requestMsg)
    
    if DEBUG:
        print('Sending data request success')


def dataRequestValidity():
    print('Data Request Watcher - Data Request Check Start')
    global unfilledDataRequests, dataRequestCount, dataRequestTimestamp
    
    
    reqRate=rospy.Rate(0.1)    
    requestTimeLimitSec = 20    # [s]
    
    
    while not rospy.is_shutdown():
        currentTimestamp = datetime.datetime.now()
                
        for req in list(unfilledDataRequests.keys()):
            secDifference = (currentTimestamp - dataRequestTimestamp[req]).total_seconds()
            
            print('Request ' + str(req) + ':', dataRequestTimestamp[req])
            print('Time difference:', secDifference, '[s]')
            
            if secDifference > requestTimeLimitSec:
                del unfilledDataRequests[req]
                del dataRequestCount[req]
                del dataRequestTimestamp[req]
        
        reqRate.sleep()
        
        
    print('Data Request Check Stop')
           
            
def listener(dji_name = "matrice300"):
    global boardId
    global dbStartTime
    
    dronename = dji_name
    boardId = dji_name.split('_', 1)[1]
    
    nodename = dronename.replace('/', '') + '_db'    
    print('DatabaseHandler', nodename)
    dbStartTime = datetime.datetime.now()  
    
    rospy.init_node(nodename)
    
    # Database switch & Data Request
    rospy.Subscriber(dji_name+'/DatabaseActive', Bool, databaseActiveCB)  
    rospy.Subscriber(dji_name+'/DataRequest', DatabasePacketRequest, dataRequestCB)
    
    print('Starting Data Request Watcher...')
    requestProgrammer = Thread(target=dataRequestValidity)
    requestProgrammer.start()
    
    # Topics to be saved
    rospy.Subscriber('/master_discovery/linkstats', LinkStatesStamped, savePacketCB)
     
    rospy.Subscriber(dji_name+'/BuildMapRequest', BuildMap, savePacketCB)
    
    rospy.Subscriber(dji_name+'/DroneHardware', DroneHardware, savePacketCB)    
    rospy.Subscriber(dji_name+'/TerminalHardware', TerminalHardware, savePacketCB)
    rospy.Subscriber(dji_name+'/DroneMovement', DroneMovement, savePacketCB)
    rospy.Subscriber(dji_name+'/DroneState', String, savePacketCB)
    
    rospy.Subscriber(dji_name+'/ESC_data', EscData, savePacketCB)
    
    rospy.Subscriber(dji_name+'/Mission', MissionDji, savePacketCB)
    rospy.Subscriber(dji_name+'/ObtainControlAuthority', Bool, savePacketCB)
    
    rospy.Subscriber(dji_name+'/Positioning', Positioning, savePacketCB)
    
    rospy.Subscriber(dji_name+'/Telemetry', Telemetry, savePacketCB)

    rospy.Subscriber(dji_name+'/WeatherStation', trisonica_msg, savePacketCB)
    
    rospy.Subscriber(dji_name+'/acceleration_ground_fused', Vector3Stamped, savePacketCB)
    rospy.Subscriber(dji_name+'/angular_velocity_fused', Vector3Stamped, savePacketCB)
    rospy.Subscriber(dji_name+'/attitude', QuaternionStamped, savePacketCB)
    
    rospy.Subscriber(dji_name+'/battery_state', BatteryState, savePacketCB)
    
    rospy.Subscriber(dji_name+'/flight_anomaly', FlightAnomaly, savePacketCB)
    
    rospy.Subscriber(dji_name+'/gimbal_angle', Vector3Stamped, savePacketCB)
    
    rospy.Subscriber(dji_name+'/wind_data', WindData, savePacketCB)
    
    rospy.Subscriber(dji_name+'/CameraList', ComponentList, savePacketCB)  
    
    rospy.Subscriber(dji_name+'/Topic', PacketType, savePacketCB)
    
    print('Database Handler - Finished topic subscription')    
    print('DatabaseHandler - Armed')
    
    atexit.register(exit_handler)

    rospy.spin()
    requestProgrammer.join()


if __name__=='__main__':
    print('Initializing Database Handler...')
    f = open("/var/lib/dbus/machine-id", "r")
    boardId = f.read().strip()[0:4]
    
    dronename = '/matrice300' + '_' + boardId
    print('DatabaseHandler', dronename, dbInstance)
    listener(dronename)
    
