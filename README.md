# ros-database-synchronization
Multi-system database logging and synchronization for ROS

## Table of Contents

- **[Introduction](#Dependencies)**<br>
- **[Installation](#Installation)**<br>
- **[Logging](#Logging)**<br>
- **[Synchronization](#Synchronization)**<br>
- **[Contributions](#Contributions)**<br>

## Dependencies
sqlite3

## Installation
Simply let the script run in the background following the same steps as any other ros enabled program.

## Logging
Place a subscriber in the subscribeTopics function inside databaseHandler.py with the following format:
	
rospy.Subscriber('/TopicName', TopicType, savePacketCB)

The incoming packets will be saved in a database where the script is located. The table name is going to be identical to the packet's type.

Thus packets that share the same type share the same database table.

## Synchronization
Following the above steps, the packet should include a seq, and uid field. An example is provided.

The packets are propagated and synchronised based on their packet type. It is necessary that a topic shares the name of the type of packets it handles. 

Thus if we have the following packet;

Hardware.msg

that is published to the following topic;

/Robot1/Hardware

then the specific packet will also be propagated to the following topics;

/Robot2/Hardware
/Robot3/Hardware
/Car1/Hardware

## Contributions

[Christos Georgiades](https://github.com/kitos2) - November, 2022
