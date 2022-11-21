# Seq number is used to differentiate packets from the same source
int64 seq
# UID is the unique identifier of each system
string uid

# When both of the above fields exist in a packet, the packet can be synchronized and logged. 
#If not, the packet is only locked locally without being propagated to the rest of the systems in the ROS network

uint32 timestamp
