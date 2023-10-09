#!/usr/bin/env
# -*- coding: utf-8 -*-
# author: Christos Georgiades
# contact: chr.georgiades@gmail.com
# date: 15-11-2022

import os
import sys
import time
import datetime

import atexit


from threading import Thread, Lock

import rospy
from kios.msg import DatabasePacketRequest

import databaseUtils as dbUtils
from databasePacketImports import *

################
    

# Propagates data to topics of different nodes but with the same name
# from /Robot1/Metrics to /Robot2/Metrics
def propagateData(packet, tableName):
    if dbUtils.DEBUG:     
        print('====\nPropagating data:\t' + tableName)
        #print(tableName)
        print(str(packet) + '\n')
    
    topics = rospy.get_published_topics()
    
    propagatedTopics = []
    #print('\nAll Valid ROS topics:\n' + str(topics))

    for topic in topics:
        if not dbUtils.dbInstanceName in topic[0]:          
            if dbUtils.DEBUG:
                print('Topic Name:\t' + str(topic))
            
#            timeStarted = time.time() 
#            topicType = dbUtils.getRostopicType(topic[0])
#            timeDelta = time.time() - timeStarted
#            
#            if dbUtils.DEBUG:
#                print('getRostopicInfo - timeDelta:\n' + str(timeDelta) + '\ntopicType:\t' + str(topicType))
                        
            if tableName in topic[0]:
                if dbUtils.DEBUG:
                    print('Propagating to:\n\t' + topic[0] + '\t' + tableName)
                    
                #print('Globals:\n' + str(globals()))
                #ros_msg_type = globals()[topicType]
                ros_msg_type = getattr(sys.modules[__name__], tableName)
                print('tempPub - ros_msg_type:\t' + str(ros_msg_type))
                    
                tempPub = rospy.Publisher(topic[0], ros_msg_type, queue_size=1)
                tempPub.publish(packet)
                
                #rospy.
                
                propagatedTopics.append(topic[0])
                
    if dbUtils.DEBUG:
        print('Topics Propagated to:\n' + str(propagatedTopics) + '\n')       
        print('Data Propagation Finished\n====')
        
    return propagatedTopics


# Checks if data is complete or missing packets
# Creates a data request for missing packets
unfilledDataRequests = {}
dataRequestCount = {}
dataRequestTimestamp = {}
dataRequestMutex = Lock() 
def checkDataCompletion(packet, tableName):
    global unfilledDataRequests, dataRequestCount, dataRequestTimestamp
    
    print('checkDataCompletion STARTTTT')

    # Check if the incoming packet fulfills a data request
    uidTable = packet.uid + '_' + tableName
    dataRequestMutex.acquire()
    if uidTable in unfilledDataRequests:
        print('\n====\nDatarequest in progress. Table: ' + str(tableName))
        print(unfilledDataRequests[uidTable])
        print('Datarequest Count:', dataRequestCount[uidTable])
        print('Creation Timestamp:', dataRequestTimestamp[uidTable])
        curTime = datetime.datetime.now()
        print('Current timestamp:', curTime, 'Timestamp Difference:', curTime - dataRequestTimestamp[uidTable])
        print('====Datarequest====\n')
        
        if dbUtils.DEBUG: 
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
            
            dataRequestMutex.release()
            
            
            #print(unfilledDataRequests)
            #del unfilledDataRequests[uidTable]
            #print(unfilledDataRequests)
            
        else:
            if dbUtils.DEBUG:
                print('===================================')
                print('Data Request unfulfilled - Awaiting')
                
                print('unfilledDataRequests[uidTable]:', unfilledDataRequests[uidTable])
                print('unfilledDataRequests[uidTable]:', dataRequestCount[uidTable])
                print('dataRequestTimestamp[uidTable]:', dataRequestTimestamp[uidTable])
                print('===================================')
            dataRequestMutex.release()
            return
    
    # If a data request for the specific table is not in process, identify any possible gaps in data
    else:
        print('Datarequest not in progress. Checking for gaps in data for table: ' + str(tableName))
        
        dataRequestMutex.release()
        requestList = buildRequestList(packet, tableName) 
        
        if dbUtils.DEBUG:
            print('Request List:', requestList)
        
        if len(requestList) > 0:
            if dbUtils.DEBUG:
                print('Packet Request List for table ' + tableName + '\n\t' + str(requestList))
             
            # Reverse list so more recent packets are requested first    
            requestList.reverse()
            
            dataRequestMutex.acquire()
            unfilledDataRequests[uidTable] = 0
            dataRequestMutex.release()
        
            for packetRequest in requestList:
                sendMissingDataRequest(packet, tableName, packetRequest)
                
#                packetRequest.timestamp = datetime.datetime.now()
#                packetRequest.count = 0
                
#                setattr(packetRequest, 'timestamp', datetime.datetime.now())
#                setattr(packetRequest, 'count', 0)
                dataRequestMutex.acquire()
                unfilledDataRequests[uidTable] = packetRequest
                dataRequestCount[uidTable] = 0
                dataRequestTimestamp[uidTable] = datetime.datetime.now()
                dataRequestMutex.release()

                
                print('Packet Request before being saved:', packetRequest)
                
    print('checkDataCompletion ENDDDDDDD')
                

# Finds missing data gaps and creates data requests for them
def buildRequestList(packet, tableName):
    query = "SELECT seq, uid FROM " + tableName + " WHERE uid=\'" + packet.uid + "\' ORDER BY seq DESC"
    if dbUtils.DEBUG:
        print('Check data completion called')
        print('Select Query:\n\t' + str(query))
    
    
    # Retrieve seq numbers from DB   
    seqList = dbUtils.executeQueryFetch(query, rowfactory = lambda cursor, row: row[0])
    
    if dbUtils.DEBUG:
        print ('seqlist for table ' + str(tableName) + '/' + str(packet.uid) + ' :', seqList)
        print('seqList built')
        print(seqList)
    
    # Find gaps in the seq numbers
    # Create a list of the needed packets to fulfill the gaps
    requestList = []   
    iMax = 0
    iMin = 1
    if len(seqList) == 1:
        if seqList[0] > 1:
            requestList.append(createPacketRequest(dbUtils.dbInstanceName, tableName, 0, seqList[0] - 1))
    else:
        while iMin < len(seqList):
            if seqList[iMax] - seqList[iMin] > 1:
                requestList.append(createPacketRequest(dbUtils.dbInstanceName, tableName, seqList[iMin] + 1, seqList[iMax] - 1))              
            elif iMin == len(seqList) - 1 and seqList[iMin] > 1:
                requestList.append(createPacketRequest(dbUtils.dbInstanceName, tableName, 0, seqList[iMax] - 1))

            iMax += 1
            iMin += 1 
            
    return requestList

# Creates a data request packet
def createPacketRequest(dronename, table, from_seq, to_seq):
    packetReq = DatabasePacketRequest()
    packetReq.sender = dronename
    packetReq.table = table
    
    packetReq.from_seq = from_seq
    packetReq.to_seq = to_seq
    
    return packetReq
    

# Publishes data request
def sendMissingDataRequest(packet, tableName, requestMsg):   
    topicName = '/matrice300_' + packet.uid + '/DataRequest' 
    
    if dbUtils.DEBUG:
        print('Sending data request:\n\t' + topicName)
    
    dataReqPub = rospy.Publisher(topicName, DatabasePacketRequest, queue_size=1, latch=True)    
    dataReqPub.publish(requestMsg)
    
    if dbUtils.DEBUG:
        print('Sending data request success')



# Separate thread function that checks if data requests are out of data
def dataRequestValidity():
    global unfilledDataRequests, dataRequestCount, dataRequestTimestamp
    
    print('Data Request Watcher - Data Request Check Start')
    
    reqRate=rospy.Rate(1)    
    requestTimeLimitSec = 10    # [s]
    
    
    while not rospy.is_shutdown():
        currentTimestamp = datetime.datetime.now()
        
        #print('dataRequestValidator ACTIVE')
        
        dataRequestMutex.acquire()                        
        for req in list(unfilledDataRequests.keys()):
            secDifference = (currentTimestamp - dataRequestTimestamp[req]).total_seconds()
            
            #if dbUtils.DEBUG:
            print('Request ' + str(req) + ':', dataRequestTimestamp[req])
            print('Time difference:', secDifference, '[s]')
            
            if secDifference > requestTimeLimitSec:
                print('Datarequest ' + str(req) + ' timed out. Deleting...')
                
                del unfilledDataRequests[req]
                del dataRequestCount[req]
                del dataRequestTimestamp[req]
                
        dataRequestMutex.release()
        
        reqRate.sleep()
        
        
    print('Data Request Watcher Stop')


def dataRequestCleanup():
    pass

def startDataRequestWatcher():    
    print('Starting Data Request Watcher...')
    
    atexit.register(dataRequestCleanup)
    
    requestProgrammer = Thread(target=dataRequestValidity)
    requestProgrammer.start()