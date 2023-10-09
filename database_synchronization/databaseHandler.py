#!/usr/bin/env
# -*- coding: utf-8 -*-
# author: Christos Georgiades
# contact: chr.georgiades@gmail.com
# date: 15-11-2022

import os
import sys
import datetime

import time

import atexit

from threading import Thread, Lock

import rospy

import databaseUtils as dbUtils
import databaseLogging as dbLog
import databaseSynchronization as dbSync
from databasePacketImports import *

#import pdb


################
DEBUG = 0
enableDBSync = 0

dbUtils.DEBUG = DEBUG


elapsed_PacketTypeSum = 0
elapsed_GetTableSum = 0
elapsed_SaveDataSum = 0
elapsed_SynchroniseSum = 0
elapsedTimeSum = 0

# Entry point for packets to be saved in database
def savePacketCB(packet):
    global databaseActive, enableDBSync
    global entryCount
    global elapsedTimeSum, elapsed_PacketTypeSum, elapsed_GetTableSum, elapsed_SaveDataSum, elapsed_SynchroniseSum
    #global elapsed_PacketType, elapsed_GetTable, elapsed_SaveData
    
    import time
    start_time = time.time()
    
    if dbUtils.DEBUG:
        print('savePacketCB - Received Packet (' + str(type(packet)) + ')\ndatabaseActive: ' + str(databaseActive) + '\nrospy.is_shutdown(): ' + str(rospy.is_shutdown()) + '\n')
    
    if databaseActive and not rospy.is_shutdown():
        #print('enableDBSync:', enableDBSync, 'packet.uid:', packet.uid)
        if not enableDBSync:
            if hasattr(packet, 'uid'):
                if packet.uid != boardId:
                    print('Database Synchronization is off but a database is trying to propagate data to me. Dropping packet')
                    return
        
        if dbUtils.DEBUG:
            print(str(type(packet)))
        
        start_time_PacketType = time.time()
        packetType = dbUtils.getPacketType(packet)
        end_time_PacketType = time.time()
        elapsed_PacketType = end_time_PacketType - start_time_PacketType
        
        # Increment Counter of received packets
        if packetType not in entryCount:
            entryCount[packetType] = 0        
        entryCount[packetType] = entryCount[packetType] + 1
          
        # Create table for the specific packet type if it does not exist yet
        start_time_GetTable = time.time()
        tableNames = dbLog.getTableNames() 
        if packetType not in tableNames:
            dbLog.createTable(packet, packetType)
        end_time_GetTable = time.time()
        elapsed_GetTable = end_time_GetTable - start_time_GetTable
          
        # Save Data
        start_time_SaveData = time.time()
        dbLog.saveData(packet, packetType)
        #dbLog.saveDataBuffer(packet, packetType)
        end_time_SaveData = time.time()
        elapsed_SaveData = end_time_SaveData - start_time_SaveData
        
        # Synchronise databases
        start_time_Synchronise = time.time()
        if enableDBSync:
            if hasattr(packet, 'seq') and hasattr(packet, 'uid'):
                propagatedTopics = None
                
                if packet.uid == boardId:
                    propagatedTopics = dbSync.propagateData(packet, packetType)
                else:
                    #print('propagatedTopics:', propagatedTopics)
                    #if propagatedTopics is not None:
                    dbSync.checkDataCompletion(packet, packetType)
        end_time_Synchronise = time.time()
        elapsed_Synchronise = end_time_Synchronise - start_time_Synchronise
                    
    end_time = time.time()
    elapsed = end_time - start_time
    elapsedTimeSum = elapsedTimeSum + elapsed
    
    elapsed_PacketTypeSum = elapsed_PacketTypeSum + elapsed_PacketType
    elapsed_GetTableSum = elapsed_GetTableSum + elapsed_GetTable
    elapsed_SaveDataSum = elapsed_SaveDataSum + elapsed_SaveData
    elapsed_SynchroniseSum = elapsed_SynchroniseSum + elapsed_Synchronise
    
    # print('savePacketCB - Elapsed Time:\t' + str(elapsed)
    # + '\n\telapsed_PacketType:\t' + str(elapsed_PacketType)
    # + '\n\telapsed_GetTable:\t' + str(elapsed_GetTable)
    # + '\n\telapsed_SaveData:\t' + str(elapsed_SaveData)
    # + '\n\telapsed_Synchronise:\t' + str(elapsed_Synchronise)) #/
    #+ '\n\telapsed_PacketType:\t' + str(elapsed_PacketType))


# Entry point for packet requests from other databases
requestTableLock = {}  
requestTableMutex = Lock()    
def dataRequestCB(dataRequest):
    global requestTableLock

    if not enableDBSync or rospy.is_shutdown():
        return
    
    if dbUtils.DEBUG:
        print('Received data request:', dataRequest)
    
    senderTable = (str(dataRequest.sender) + '/' + str(dataRequest.table))
    if dbUtils.DEBUG:
        print('Sender Table:', senderTable)
    if senderTable in requestTableLock:
        print('senderTable Value:' + str(requestTableLock[senderTable]))
    
    requestTableMutex.acquire()
    if senderTable not in requestTableLock:
        requestTableLock[senderTable] = True
    elif requestTableLock[senderTable]:
        if dbUtils.DEBUG:
            print('Data request already in progress for table ' + dataRequest.table)
        
        requestTableMutex.release() 
        return
    elif not requestTableLock[senderTable]:
        requestTableLock[senderTable] = True
        
    requestTableMutex.release() 
    
    
    if dbUtils.DEBUG:
        print('\nGot a request for data from ' + str(dataRequest.sender) + '\nasking for entries [' + str(dataRequest.from_seq) + ':' + str(dataRequest.to_seq) + '] from table ' + str(dataRequest.table))   
    
    query = "SELECT * FROM " + str(dataRequest.table) + " WHERE uid=\'" + str(boardId) + "\' AND seq BETWEEN " + str(dataRequest.from_seq) + " AND " + str(dataRequest.to_seq)
    
    if dbUtils.DEBUG:
        print('Data Retrieval Query:\n\t' + str(query))
    
    resultList = dbUtils.executeQueryFetch(query)

    
    topic = dataRequest.sender + '/' + dataRequest.table
    if dbUtils.DEBUG:
        print ('DataRequest - Publishing to: ' + topic)
        
    datarequestPub = rospy.Publisher(topic, getattr(sys.modules[__name__], str(dataRequest.table)), queue_size=10)
    
    #packet_class = globals()[dataRequest.table]
    
    path = os.path.abspath(os.getcwd())
    if dbUtils.DEBUG:
        print('Current Path:', path)
        print('Looking for:', dataRequest.table)
    packetMsg = dbUtils.findMsgFile(dataRequest.table + '.msg') #find(dataRequest.table + '.msg', path)
    
    if packetMsg:
        #datafields, datatypes = readMsgFile(packetMsg)
        
        if len(resultList) > 0:
            packetList = dbUtils.deserializeRosPacketRecursive(resultList, dataRequest.table)
            
            for packet in packetList:
                datarequestPub.publish(packet)
                
                if dbUtils.DEBUG or True:
                    print('datarequestcb - Just published packet ' + str(packet))
        
    
    #print('Entry List END')  
    requestTableMutex.acquire()    
    requestTableLock[senderTable] = False
    requestTableMutex.release() 
    
    if dbUtils.DEBUG:
        print('Data Request Finished')


# Sets the state of the database
databaseActive = True           
def databaseActiveCB(dbState):
    global databaseActive
    
    print('databaseActiveCB', dbState)
    
    if dbState.data:
        databaseActive = True
    else:
        databaseActive = False      
        
    print('Database State - ' + str(databaseActive))
    
    
# Sets the synchronization of the database          
def databaseSyncActiveCB(dbState):
    global enableDBSync
    
    print('databaseSyncActiveCB', dbState)
    
    if dbState.data:
        enableDBSync = True
    else:
        enableDBSync = False      
        
    print('Database Synchronization State - ' + str(enableDBSync))

            
def listener(dji_name = "matrice300"):
    global boardId
    global flightlog_path   
    
    dronename = dji_name
    boardId = dji_name.split('_', 1)[1]
    nodename = dronename.replace('/', '') + '_db'
    
    dbUtils.Init(dronename)
    dbLog.Init()
    dbSync.startDataRequestWatcher()
    print('DatabaseHandler', dronename, dbUtils.dbInstance)
    print('DatabaseHandler', nodename)

    
    # Database switch & Data Request
    rospy.Subscriber(dronename+'/DatabaseActive', Bool, databaseActiveCB)
    rospy.Subscriber(dronename+'/DatabaseSynchronizationActive', Bool, databaseSyncActiveCB)       
    rospy.Subscriber(dronename+'/DataRequest', DatabasePacketRequest, dataRequestCB)

    subscribeTopics()
 
    print('Database Handler - Finished topic subscription')    
    print('DatabaseHandler - Armed')
    
    atexit.register(dumpDBMetrics)


    measurements_topic = '/crps/Telemetry'
    topic_list = []
    # while not rospy.is_shutdown():
    #     rospy.get_published_topics()

    #     for topic in topics:
    #         if measurements_topic in topic[0] and measurements_topic != topic[0] and not dronename in topic[0]:
    #             if not topic[0] in topic_list:
    #                 rospy.Subscriber(topic[0], Telemetry, savePacketCB)
    #                 topic_list.append(topic[0])


    rospy.spin()


# Topics to be saved
def subscribeTopics():
    dronename = dbUtils.dbInstanceName
    
    # Packet Subscription Example
    #rospy.Subscriber(dronename+'/Topic', PacketType, savePacketCB)

    rospy.Subscriber(dronename+'/Telemetry', Telemetry, savePacketCB)

    print('Database Handler - Topic Registration completed')
    
    


# Print DB metrics at exit()
entryCount = {}
#requestCount = {}
def dumpDBMetrics():
    tableNames = dbLog.getTableNames()
    
    time.sleep(3)
    
    completedEntries = 0        
    dbCompleteness = 0
    for table in tableNames:
        query = 'SELECT COUNT(*) FROM ' + table
        
        dbResult = dbUtils.executeQueryFetch(query, rowfactory = lambda cursor, row: row[0])
        completedEntries = completedEntries + dbResult[0] * 1.0
        completeness = dbResult[0] * 1.0 / entryCount[table] * 100.0
        
        print(table + '\t' + str(dbResult[0]) + '/' + str(entryCount[table]) + '\t' + str(completeness) + '%')
        
        dbCompleteness = dbCompleteness + completeness
         
    if len(tableNames) > 0:
        print('Database Completeness\t' + str(dbCompleteness/len(tableNames)) + '%\n')
    else:
        print('Database Completeness\t' + str(0) + '%\tDid not log.\n')
    
    dbEndTime = datetime.datetime.now()
    dbRuntime = dbEndTime - dbUtils.dbStartTime                         
    duration_in_s = dbRuntime.total_seconds()   
    
    
    entriesPerSecond = completedEntries / duration_in_s
    if completedEntries > 0:
        print('Average Time per Entry:\t' + str(elapsedTimeSum / completedEntries) + '[s]')
        
        print('\tAverage PacketType:\t' + str(elapsed_PacketTypeSum / completedEntries) + '[s]')
        print('\tAverage GetTable:\t' + str(elapsed_GetTableSum / completedEntries) + '[s]')
        print('\tAverage SaveData:\t' + str(elapsed_SaveDataSum / completedEntries) + '[s]')
        print('\tAverage Synchronise:\t' + str(elapsed_SynchroniseSum / completedEntries) + '[s]')
    
    
#    elapsed_PacketTypeSum = elapsed_PacketTypeSum + elapsed_PacketType
#    elapsed_GetTableSum = elapsed_GetTableSum + elapsed_GetTable
#    elapsed_SaveDataSum = elapsed_SaveDataSum + elapsed_SaveData
#    elapsed_SynchroniseSum = elapsed_SynchroniseSum + elapsed_Synchronise
    
    print('DB entries per second:\t' + str(entriesPerSecond) + '[entry/s]')  
    print('DB runtime:\t' + str(duration_in_s) + '[s]')


if __name__=='__main__':
    print('Initializing Database Handler...')
    f = open("/var/lib/dbus/machine-id", "r")
    boardId = f.read().strip()[0:4]
    
    dronename = '/matrice300' + '_' + boardId
    listener(dronename)
    
