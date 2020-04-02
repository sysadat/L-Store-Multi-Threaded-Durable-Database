from lstore.disk import *
from lstore.buffer import *
from time import time
import lstore.config
from pathlib import Path
import threading
import os
import sys
import pickle
import copy
import queue

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
BASE_RID_COLUMN = 3

#only return if there is a page directory file specified, only happens after the db has been closed
def read_page_directory(name):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/page_directory.pkl"
    if os.path.exists(file_name):
        with open(file_name, 'rb') as file:
            return pickle.load(file)
    else:
        return {} #just return empty dict

def write_page_directory(name, page_directory):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/page_directory.pkl"
    with open(file_name, "wb") as file:
        pickle.dump(page_directory, file)

def read_counters(name):
    counters = []
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/counters"
    if os.path.exists(file_name):
        with open(file_name, "rb") as file:
            for counter in range(6):
                counters.append(int.from_bytes(file.read(8), "big"))
        return counters

def write_counters(name, counters):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/counters"
    with open(file_name, "wb") as file:
        for counter in counters:
            file.write((counter).to_bytes(8, "big"))

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :param base_RID/tail_RID    #start indexes for records for tail and base ranges
    :param base_range/tail_range #in memory representation of page storage
    """
    def __init__(self, name, num_columns, key, buffer_pool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.sum = 0
        self.buffer = buffer_pool
        self.disk = None
        self.page_directory = {}
        self.read_lock_manager = {}
        self.write_lock_manager = {}
        self.read_lock_manager_latch = False
        self.write_lock_manager_latch = False
        self.merge_queue = queue.Queue()

        self.base_RID = lstore.config.StartBaseRID
        self.tail_RID = lstore.config.StartTailRID
        self.base_RID_lock = 0
        self.tail_RID_lock = 0
        self.base_offset_counter = 0
        self.tail_offset_counter = 0

    def acquire_read(self, rid):
        #print("acquire_read started")
        # thread_lock = threading.RLock()

        # thread_lock.acquire()
        while self.read_lock_manager_latch == True or self.write_lock_manager_latch == True:
            pass

        self.write_lock_manager_latch = True
        self.read_lock_manager_latch = True
        if rid in self.write_lock_manager: 
            if threading.get_ident() == self.write_lock_manager[rid]:
                if self.read_lock_manager[rid] == None:
                    # print("acquiring first new read lock on rid " + str(rid))
                    self.read_lock_manager[rid] = [threading.get_ident()]
                elif threading.get_ident() not in self.read_lock_manager[rid]: #only append if not in read lock list for record
                    # print("appending read lock because not in list")
                    self.read_lock_manager[rid].append(threading.get_ident())
                else:
                    # print("read lock already exists")
                    self.write_lock_manager_latch = False
                    self.read_lock_manager_latch = False
                    # thread_lock.release()
                    return True
            else:
                #print("acquiring read lock on value with a non-native exclusive lock")
                self.write_lock_manager_latch = False
                self.read_lock_manager_latch = False
                # thread_lock.release()
                return False #rid is in the outstanding writes, being used by another thread
        elif rid in self.read_lock_manager:
            if threading.get_ident() in self.read_lock_manager[rid]:
                # print("read lock already exists")
                self.write_lock_manager_latch = False
                self.read_lock_manager_latch = False
                # thread_lock.release()
                return True
        else:
            # print("New read lock on thread " + str(threading.get_ident()) + " for rid " + str(rid))
            self.read_lock_manager[rid] = [threading.get_ident()] #no write lock on record, can read
            self.write_lock_manager_latch = False
            self.read_lock_manager_latch = False
            # thread_lock.release()
            return True

        self.write_lock_manager_latch = False
        self.read_lock_manager_latch = False

    def acquire_write(self, rid):
        #print("acquire_write started")
        #thread_lock = threading.RLock()

        # thread_lock.acquire()
        while self.read_lock_manager_latch == True:
            pass

        self.read_lock_manager_latch = True
        for thread in threading.enumerate():
            if rid in self.read_lock_manager: #key exists in read lock manager
                if thread.ident in self.read_lock_manager[rid] and thread.ident != threading.get_ident(): #check if outstanding read exists
                    # print("outstanding read exists on this record from a different txn")
                    # thread_lock.release()
                    self.read_lock_manager_latch = False
                    return False
        self.read_lock_manager_latch = False
        # thread_lock.release()

        # thread_lock.acquire()
        while self.write_lock_manager_latch == True:
            pass

        self.write_lock_manager_latch = True
        if rid in self.write_lock_manager:
            # print("rid " + str(rid) + " exists in the lock manager on thread " + str(threading.get_ident()))
            if threading.get_ident() == self.write_lock_manager[rid]:
                self.write_lock_manager_latch = False
                # thread_lock.release()
                return True
            else:
                self.write_lock_manager_latch = False
                # thread_lock.release()
                return False
        else:
            # print("no write lock for rid " + str(rid) + " in the write lock manager, creating new in thread " + str(threading.get_ident()))
            self.write_lock_manager[rid] = threading.get_ident() #no current write locks on record
            self.write_lock_manager_latch = False
            # thread_lock.release()
            return True

    def release_locks(self):
        #print("release_locks started")
        # thread_lock = threading.RLock()
        write_locks_to_delete = []
        read_locks_to_delete = []

        # thread_lock.acquire()
        while self.read_lock_manager_latch == True:
            pass

        self.read_lock_manager_latch = True
        for rid in list(self.read_lock_manager.keys()):
            if threading.get_ident() in self.read_lock_manager[rid]:
                read_locks_to_delete.append(rid)
        self.read_lock_manager_latch = False
        # thread_lock.release()

        # thread_lock.acquire()
        while self.write_lock_manager_latch == True:
            pass

        self.write_lock_manager_latch = True
        for rid in list(self.write_lock_manager.keys()):
            if threading.get_ident() == self.write_lock_manager[rid]:
                write_locks_to_delete.append(rid)
        self.write_lock_manager_latch = False
        # thread_lock.release()

        while self.read_lock_manager_latch == True or self.write_lock_manager_latch == True:
            pass

        self.read_lock_manager_latch = True
        self.write_lock_manager_latch = True

        for rid in read_locks_to_delete:
            # thread_lock.acquire()
            self.read_lock_manager[rid].remove(threading.get_ident())
            # thread_lock.release()
        for rid in write_locks_to_delete:
            # thread_lock.acquire()
            del self.write_lock_manager[rid]
            # thread_lock.release()

        self.read_lock_manager_latch = False
        self.write_lock_manager_latch = False
        #print("release_locks finished\n")

    #TODO: implement TPS calculation
    def __merge__(self, base_range_copy, tail_range_offsets):
        for tail_range_offset in reversed(tail_range_offsets): #for every range reversed
            tail_range = self.buffer.fetch_range(self.name, tail_range_offset)
            for record_index in range(lstore.config.PageEntries - 1, 0, -1): #for each record backwards, starting at index 511 (PageEntries - 1)

                tail_rid = tail_range[RID_COLUMN].read(record_index)
                base_rid_for_tail = tail_range[BASE_RID_COLUMN].read(record_index)
                if tail_rid == 0: #if the tail record has been invalidated by an aborted transaction
                    print("deleted tail record found, skipping")
                    continue

                _ , base_record_index = self.page_directory[base_rid_for_tail] #retrieve record index
                base_indirection = base_range_copy[INDIRECTION_COLUMN].read(base_record_index) #get indirection column for comparison

                if(base_indirection == tail_rid): #tail record is indeed the latest one
                    for column_index in range(lstore.config.Offset, lstore.config.Offset + self.num_columns): #for each column
                            tail_page_value = tail_range[column_index].read(record_index)
                            base_range_copy[column_index].inplace_update(base_record_index, tail_page_value)
            self.buffer.unpin_range(self.name, tail_range_offset)

        last_tail_range_offset = tail_range_offsets[len(tail_range_offsets) - 1] #last offset 
        last_tail_range = self.buffer.fetch_range(self.name, last_tail_range_offset)
        tps_value = last_tail_range[RID_COLUMN].read(lstore.config.PageEntries - 1) #last record in last page is the TPS
        self.buffer.unpin_range(self.name, last_tail_range_offset)

        return (base_range_copy, tps_value)

    def __prepare_merge__(self, base_offset):
        base_range_copy = copy.deepcopy(self.buffer.fetch_range(self.name, base_offset)) #create a separate copy of the base range to put in the bg thread
        next_offset = self.disk.get_offset(self.name, 0, base_offset)
        self.buffer.unpin_range(self.name, base_offset) #update is finished, unpin
        tail_ranges = []

        counter = 0
        previous_offset = next_offset
        tail_offset = next_offset

        while counter != lstore.config.TailMergeLimit and tail_offset != 0:
            tail_ranges.append(tail_offset)
            previous_offset = tail_offset
            tail_offset = self.disk.get_offset(self.name, 0, previous_offset)
            counter += 1

        if counter < lstore.config.TailMergeLimit:
            # not enough pages to merge
            print("somehow counter is les than the tail merge limit, traversal error")
            return

        consolidated_range, tps_value = self.__merge__(base_range_copy, tail_ranges) #initiate merge, return a consolidated range
        base_range = self.buffer.fetch_range(self.name, base_offset)

        #get metadata
        consolidated_range = base_range[:lstore.config.Offset] + consolidated_range[lstore.config.Offset:] #store the base metadata columns and the merged data columns
        self.buffer.unpin_range(self.name, base_offset) #unpin base_range after consolidation
        new_offset = (tail_offset if tail_offset == 0 else previous_offset) #either get the last tail_page's offset to point the base page to, or the previous offset if there is no next tail page
        for column_index in range(lstore.config.Offset + self.num_columns):
            consolidated_range[column_index].update_tps(tps_value) #update the tps in the consolidated pages before assignment
            self.disk.update_offset(self.name, column_index, base_offset, tail_offset) #update the offset of the ranges

        # for tail_range in tail_ranges:
        #     for column_index in range(lstore.config.Offset + self.num_columns):
        #         tail_range[column_index].empty_page() #empty out the tail pages

        while self.buffer.is_pinned(self.name, base_offset):
            print("pin count on range is " + str(self.buffer.get_pins(self.name, base_offset)))

        self.buffer.page_map[self.buffer.frame_map[base_offset]] = consolidated_range #update buffer_pool

    def __add_physical_base_range__(self): #calling function is thread safe
        lock = threading.RLock()
        lock.acquire()
        if self.base_offset_counter < self.tail_offset_counter:
            self.base_offset_counter = self.tail_offset_counter + lstore.config.FilePageLength
        else:
            self.base_offset_counter += lstore.config.FilePageLength #increase offset after adding a range
        self.buffer.add_range(self.name, self.base_offset_counter)
        lock.release()

    def __add_physical_tail_range__(self, previous_offset_counter): #calling function is thread safe
        lock = threading.RLock()
        lock.acquire()
        if self.tail_offset_counter < self.base_offset_counter:
            self.tail_offset_counter = self.base_offset_counter + lstore.config.FilePageLength
        else:
            self.tail_offset_counter += lstore.config.FilePageLength #increase offset after adding a range
        self.buffer.add_range(self.name, self.tail_offset_counter)
        lock.release()

        for column_index in range(lstore.config.Offset + self.num_columns): #update all the offsets
            lock.acquire()
            self.disk.update_offset(self.name, column_index, previous_offset_counter, self.tail_offset_counter) #update offset value i
            lock.release()


    def __read__(self, RID, query_columns):
        lock = threading.RLock()
        # What the fick tail index and tails slots?
        tail_index = tail_slot_index = -1
        page_index, slot_index = self.page_directory[RID]

        lock.acquire()
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN] #index into the physical location
        lock.release()
        self.buffer.unpin_range(self.name, page_index)

        current_page_tps = current_page.get_tps() #make sure the indirection column hasn't already been merged
        new_rid = current_page.read(slot_index)
        column_list = []
        key_val = -1
        if new_rid != 0 and (current_page_tps == 0 or new_rid < current_page_tps):
            tail_index, tail_slot_index = self.page_directory[new_rid] #store values from tail record

            lock.acquire()
            current_tail_range = self.buffer.fetch_range(self.name, tail_index)
            lock.release()

            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    #TODO TF is this shit, does it acctually give the key val
                    key_val = query_columns[column_index - lstore.config.Offset]
                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_tail_page = current_tail_range[column_index] #get tail page from
                    column_val = current_tail_page.read(tail_slot_index)
                    column_list.append(column_val)
            self.buffer.unpin_range(self.name, tail_index) #unpin at end of transaction
        else:
            lock.acquire()
            current_base_range = self.buffer.fetch_range(self.name, page_index)
            lock.release()

            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    key_val = query_columns[column_index - lstore.config.Offset] #subtract offset for the param columns

                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_base_page = current_base_range[column_index]
                    column_val = current_base_page.read(slot_index)
                    column_list.append(column_val)
            self.buffer.unpin_range(self.name, page_index) #unpin at end of transaction

        # check indir column record
        # update page and slot index based on if there is one or nah
        return Record(RID, key_val, column_list) #return proper record, or -1 on key_val not found

    def __insert__(self, columns):
        lock = threading.RLock()
        #returning any page in range will give proper size
        lock.acquire()
        current_range = self.buffer.fetch_range(self.name, self.base_offset_counter)[0]
        lock.release()

        self.buffer.unpin_range(self.name, self.base_offset_counter) #unpin after getting value
        if not current_range.has_capacity(): #if latest slot index is -1, need to add another range
            self.__add_physical_base_range__()

        page_index = self.base_offset_counter

        lock.acquire()
        current_base_range = self.buffer.fetch_range(self.name, page_index)
        lock.release()

        for column_index in range(self.num_columns + lstore.config.Offset):
            current_base_page = current_base_range[column_index]
            slot_index = current_base_page.write(columns[column_index])
        self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory
        self.buffer.unpin_range(self.name, page_index) #unpin at the end of transaction

    #in place update of the indirection entry.
    def __update_indirection__(self, old_RID, new_RID):
        lock = threading.RLock()
        page_index, slot_index = self.page_directory[old_RID]

        lock.acquire()
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN]
        lock.release()

        current_page.inplace_update(slot_index, new_RID)
        self.buffer.unpin_range(self.name, page_index) #unpin after inplace update

    # Set base page entry RID to 0 to invalidate it
    def __delete__ (self, RID):
        lock = threading.RLock()
        page_index, slot_index = self.page_directory[RID]

        lock.acquire()
        current_page = self.buffer.fetch_range(self.name, page_index)[RID_COLUMN]
        lock.release()

        current_page.inplace_update(slot_index, 0)
        self.buffer.unpin_range(self.name, page_index) #unpin after inplace update

    def __return_base_indirection__(self, RID):
        lock = threading.RLock()
        page_index, slot_index = self.page_directory[RID]

        lock.acquire()
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN]
        lock.release()

        indirection_index = current_page.read(slot_index)
        self.buffer.unpin_range(self.name, page_index) #unpin after reading indirection
        return indirection_index

    def __traverse_tail__(self, page_index):
        counter = 0
        tail_offset = self.disk.get_offset(self.name, 0, page_index) #tail pointer at the specified base page in disk
        prev_tail = page_index
        while tail_offset != 0:
            prev_tail = tail_offset
            tail_offset = self.disk.get_offset(self.name, 0, prev_tail)
            counter += 1

        tail_offset = prev_tail
        return tail_offset, counter

    def __update__(self, columns, base_rid):
        thread_lock = threading.RLock()
        base_offset, _ = self.page_directory[base_rid]
        current_tail = None
        previous_offset, num_traversed = self.__traverse_tail__(base_offset)
        page_offset = previous_offset

        if previous_offset == base_offset: #if there is no tail page for the base page
            thread_lock.acquire()
            self.__add_physical_tail_range__(previous_offset)
            page_offset = self.tail_offset_counter
            thread_lock.release()

        thread_lock.acquire()
        current_tail = self.buffer.fetch_range(self.name, page_offset)[0]
        thread_lock.release()

        self.buffer.unpin_range(self.name, page_offset)  #just needed to read this once, unpin right after
        if not current_tail.has_capacity(): #if the latest tail page is full
            thread_lock.acquire()
            self.__add_physical_tail_range__(previous_offset)
            page_offset = self.tail_offset_counter #add the new range and update the tail offsets accordingly
            thread_lock.release()

            self.buffer.unpin_range(self.name, base_offset)

            thread_lock.acquire()
            base_range = self.buffer.fetch_range(self.name, base_offset)
            thread_lock.release()

            if (num_traversed >= lstore.config.TailMergeLimit) and (base_range[0].has_capacity() == False): # maybe should be >=, check to see if the base page is full
                
                thread_lock.acquire()
                print("merge start")
                merge_thread = (threading.get_ident() if lstore.config.merge_thread == -1 else lstore.config.merge_thread)
                thread_lock.release()
                self.buffer.unpin_range(self.name, base_offset)
                self.merge_queue.put(base_offset) #add page offset to the queue
                if len(threading.enumerate()) == 1: #TODO: check the name of the current thread, needs to make sure that merge isn't in the pool
                    thread = threading.Thread(name = "merge_thread", target = self.__prepare_merge__, args = [self.merge_queue.get()]) # needs to be called in a threaded way, pass through the deep copy of the base range
                    thread.start()

        thread_lock.acquire()
        current_tail_range = self.buffer.fetch_range(self.name, page_offset)
        thread_lock.release()

        for column_index in range(self.num_columns + lstore.config.Offset):
            current_tail_page = current_tail_range[column_index]
            slot_index = current_tail_page.write(columns[column_index])
        self.page_directory[columns[RID_COLUMN]] = (page_offset, slot_index) #on successful write, store to page directory
        self.buffer.unpin_range(self.name, page_offset) #update is finished, unpin

    def __undo_update__(self, base_rid):
        base_offset, slot_index = self.page_directory[base_rid]
        base_range = self.buffer.fetch_range(self.name, base_offset)
        tail_rid = base_range[INDIRECTION_COLUMN].read(slot_index)
        self.buffer.unpin_range(self.name, base_offset)

        tail_offset, tail_slot_index = self.page_directory[tail_rid]
        tail_range = self.buffer.fetch_range(self.name, tail_offset)

        tail_rid_col = tail_range[RID_COLUMN]
        tail_indirection_col = tail_range[INDIRECTION_COLUMN]

        tail_rid_col.inplace_update(tail_slot_index, 0) #reset the rid column TODO: make sure merge checks the RID and continues if 0
        next_latest_rid = tail_indirection_col.read(tail_slot_index)
        self.buffer.unpin_range(tail_range, tail_offset)
        # print("current tail is " + str(tail_rid) + " next latest tail_rid is " + str(next_latest_rid))
        self.__update_indirection__(base_rid, next_latest_rid)

        # lock = threading.Lock()
        # lock.acquire()
        # del self.page_directory[tail_rid]
        # lock.release()

