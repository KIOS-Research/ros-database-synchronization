#!/usr/bin/env
# -*- coding: utf-8 -*-
# author: Christos Georgiades
# contact: chr.georgiades@gmail.com
# date: 15-11-2022

try:
    #python3
    import queue
except ModuleNotFoundError:
    #python2
    import Queue as queue

import time
import atexit
import rospy

from threading import Thread, Lock

import databaseUtils as dbUtils
from databasePacketImports import *
################

bufferedWriting = 0


# Creates tables according to packet contents and type
def createTable(packet, packetType):        
    if dbUtils.DEBUG:
        print("packetType:", packetType)
    
    
    datavalues, datafields, datatypes = dbUtils.serializeRosPacketRecursive(packet)
    
    print('Creating Table of size ' + str(len(datavalues)) + '\t' + str(len(datafields)) + '\t' + str(len(datatypes)))
    
    # Create a table with the appropriate number of fields 
    query = "CREATE TABLE " + packetType + " (" + ",".join(datafields) + ")"
    if dbUtils.DEBUG:
        print(query)
        
    dbUtils.executeQuery(query)

    if dbUtils.DEBUG:        
        print(packetType + ' table created')


# Retrieve the names of created tables in the db
def getTableNames():
    global dbCursor
    
    query = "SELECT name FROM sqlite_master WHERE type='table';"
    dbResult = dbUtils.executeQueryFetch(query, rowfactory = lambda cursor, row: row[0])

    return dbResult


elapsed_serializeSum = 0
elapsed_sanitiseSum = 0
elapsed_executeQuerySum = 0


# Saves packet in db
def saveData(packet, tableName):  
    global elapsed_serializeSum, elapsed_sanitiseSum, elapsed_executeQuerySum
    
    if dbUtils.DEBUG:
        print('Converting ros packet to sql entry')
    
    start_time_serialize = time.time()    
    datavalues, datafields, datatypes = dbUtils.serializeRosPacketRecursive(packet, depth=-1)
    linecount = len(datavalues)
    end_time_serialize = time.time()
    elapsed_serialize = end_time_serialize - start_time_serialize
    
    start_time_sanitise = time.time()  
    for i in range(len(datavalues)):
        datavalues[i] = dbUtils.sanitiseData(datavalues[i])
    end_time_sanitise = time.time() 
    elapsed_sanitise = end_time_sanitise - start_time_sanitise
    
    if dbUtils.DEBUG:
        print('linecount', linecount)
        print('datafields', datavalues)
        print('datavalues', datavalues)
                
        print('\n' + tableName)
        print('datafields ' + str(len(datafields)) + '\tdatatypes ' + str(len(datatypes)) + '\tdatavalues ' + str(len(datavalues)))
        for i in range(len(datafields)):
            print(datafields[i], datatypes[i], datavalues[i])
            
        print('')        
    
    start_time_executeQuery = time.time() 
    # Create a query to save incoming data
    query = 'INSERT INTO ' + tableName +  ' VALUES('
    for i in range(linecount - 1):
        query += '?, '    
    query += '?)'
        
    dbUtils.executeQueryWrite(query, [tuple(datavalues)])
    end_time_executeQuery = time.time() 
    elapsed_executeQuery = end_time_executeQuery - start_time_executeQuery
    
    elapsed_serializeSum = elapsed_serializeSum + elapsed_serialize
    elapsed_sanitiseSum = elapsed_sanitiseSum + elapsed_sanitise
    elapsed_executeQuerySum = elapsed_executeQuerySum + elapsed_executeQuery
    

    if dbUtils.DEBUG:        
        print('Saving Data Done\t' + tableName)
        
        
# Saves packet in db
def prepareData(packet, tableName):   
    if dbUtils.DEBUG:
        print('Converting ros packet to sql entry')
        
    datavalues, datafields, datatypes = dbUtils.serializeRosPacketRecursive(packet, depth=-1)
    linecount = len(datavalues)
    
    for i in range(len(datavalues)):
        datavalues[i] = dbUtils.sanitiseData(datavalues[i])
    
    if dbUtils.DEBUG:
        print('linecount', linecount)
        print('datafields', datavalues)
        print('datavalues', datavalues)
                
        print('\n' + tableName)
        print('datafields ' + str(len(datafields)) + '\tdatatypes ' + str(len(datatypes)) + '\tdatavalues ' + str(len(datavalues)))
        for i in range(len(datafields)):
            print(datafields[i], datatypes[i], datavalues[i])
            
        print('')        
     
    return tuple(datavalues)
        

incomingPacketBuffer = {}
incomingPacketBufferMutex = Lock() 
def saveDataBuffer(packet, tableName):
    incomingPacketBufferMutex.acquire() 
    
    if tableName not in incomingPacketBuffer:
        incomingPacketBuffer[tableName] = queue.Queue()
        
    #preparedTuple = prepareData(packet, tableName)
    #incomingPacketBuffer[tableName].put(preparedTuple)
    
    incomingPacketBuffer[tableName].put(packet)
    
    incomingPacketBufferMutex.release() 
    

# Check if packet already exists in db
def checkIfEntryExists(packet, packetName):     
    query = "SELECT * FROM " + str(packetName) + " WHERE uid=\'" + packet.uid + "\' AND seq=" + str(packet.seq)
    
    if dbUtils.DEBUG:
        print(query)
        
    dbResult = dbUtils.executeQueryFetch(query, rowfactory = lambda cursor, row: row[0])
    
    if dbUtils.DEBUG:
        print("Retrieved results: " + str(len(dbResult)))
    
    if len(dbResult) > 0:
        return True
    else:
        return False 


def bufferedWriter():
    global incomingPacketBuffer, incomingPacketBufferMutex
    
    print('Buffered Writer - Start')
    
    writeRate=rospy.Rate(2)
    
    while not rospy.is_shutdown():        
        incomingPacketBufferMutex.acquire()  

        
                      
        for req in list(incomingPacketBuffer.keys()):
            writeList = []
            
            while incomingPacketBuffer[req].qsize() > 0:          
                preparedTuple = prepareData(incomingPacketBuffer[req].get(), req)
                writeList.append(preparedTuple)
                
                #writeList.append(incomingPacketBuffer[req].get())
        
        
            if len(writeList) > 0:
                if dbUtils.DEBUG:
                    print('Writing ' + str(len(writeList)) + ' entries to table ' + str(req))                  
                    print('writelist ' + str(req) + str(writeList))
                
                linecount = len(writeList[0])
                tableName = req
                
                # Create a query to save incoming data
                query = 'INSERT INTO ' + tableName +  ' VALUES('
                for i in range(linecount - 1):
                    query += '?, '    
                query += '?)'
                
                dbUtils.executeQueryWrite(query, writeList)
                
                
        incomingPacketBufferMutex.release()        
        writeRate.sleep()
               
    print('Buffered Writer - Stop')



def dumpDBMetrics():
    tableNames = getTableNames()
    
    time.sleep(5)
    
    completedEntries = 0        
#    dbCompleteness = 0
    for table in tableNames:
        query = 'SELECT COUNT(*) FROM ' + table
        
        dbResult = dbUtils.executeQueryFetch(query, rowfactory = lambda cursor, row: row[0])
        completedEntries = completedEntries + dbResult[0] * 1.0
#        completeness = dbResult[0] * 1.0 / entryCount[table] * 100.0
        
#        print(table + '\t' + str(dbResult[0]) + '/' + str(entryCount[table]) + '\t' + str(completeness) + '%')
#        
#        dbCompleteness = dbCompleteness + completeness
#         
#        
#    print('Database Completeness\t' + str(dbCompleteness/len(tableNames)) + '%\n')
#    
#    dbEndTime = datetime.datetime.now()
#    dbRuntime = dbEndTime - dbUtils.dbStartTime                         
#    duration_in_s = dbRuntime.total_seconds()   
    
    
#    entriesPerSecond = completedEntries / duration_in_s
#    print('Average Time per Entry:\t' + str(elapsedTimeSum / completedEntries) + '[s]')
    
    print('\tAverage serialize:\t' + str(elapsed_serializeSum / completedEntries) + '[s]')
    print('\tAverage sanitise:\t' + str(elapsed_sanitiseSum / completedEntries) + '[s]')
    print('\tAverage executeQuery:\t' + str(elapsed_executeQuerySum / completedEntries) + '[s]')
#    print('\tAverage Synchronise:\t' + str(elapsed_SynchroniseSum / completedEntries) + '[s]')
    
#    elapsed_serializeSum = 0
#    elapsed_sanitiseSum = 0
#    elapsed_executeQuerySum = 0
    
    
#    elapsed_PacketTypeSum = elapsed_PacketTypeSum + elapsed_PacketType
#    elapsed_GetTableSum = elapsed_GetTableSum + elapsed_GetTable
#    elapsed_SaveDataSum = elapsed_SaveDataSum + elapsed_SaveData
#    elapsed_SynchroniseSum = elapsed_SynchroniseSum + elapsed_Synchronise
    
#    print('DB entries per second:\t' + str(entriesPerSecond) + '[entry/s]')
#    
#    print('DB runtime:\t' + str(duration_in_s) + '[s]')



def Init():
    #bufferedWriterThread = Thread(target=bufferedWriter)
    #bufferedWriterThread.start()
    
    atexit.register(dumpDBMetrics)
    
    pass