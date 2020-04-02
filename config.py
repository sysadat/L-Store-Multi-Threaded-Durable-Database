# Global Setting for the Database
PageEntries = 512
PageLength = 4096
FilePageLength = 4096 + 8
StartBaseRID = 1
StartTailRID = ((2 ** 63) - 1)
Offset = 4
#Increment buffersize from 20-50, by 5 increments.  Default 20
buffersize = 100
DBName = ""
#Increment buffersize from 1-10, by 1 increments. Default 3
TailMergeLimit = 3
merge_thread = -1