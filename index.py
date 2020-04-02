from lstore.table import Table
import lstore.config
from collections import defaultdict
from lstore.btree import BTreeNode, BPTree
import threading

class Index:

    def __init__(self, table):
        self.table = table
        self.index_dict = []
        # key is primary key; value is rid
        # Want to go through num_columns and initialize Btrees

        # Create dictionaries for each of the columns
        for i in range(self.table.num_columns):
            self.index_dict.append({})

        if len(table.page_directory) != 0:
            self.create_index()

    # returns the location of all records with the given value
    # Add another parameter, column, so we can specify the column we want to find
    def locate(self, value, column):
        # BTree = index_dict[column]
        # BTree.search()
        # Locate certain values once we pass the column and the value
        if value not in self.index_dict[column]:
            print(str(value) + " not found, thread id is: " + str(threading.get_ident()))
            return []
        else:
            return self.index_dict[column][value] #return the rid value
        return -1

    # Create index on specific column
    # If key not in our dict, we addend the RID to the column, else we create new index
    def create_index(self):
        counter = 0
        for RID, values in self.table.page_directory.items():
            page_index = values[0]
            slot_index = values[1]
            current_range = self.table.buffer.fetch_range(self.table.name, page_index) #get the RID column
            current_page_rid = current_range[1].read(slot_index)
            current_page_indirection = current_range[0].read(slot_index)
            self.table.buffer.unpin_range(self.table.name, page_index)

            #print("current page rid is " + str(current_page_rid) + " current indirection is " + str(current_page_indirection))
            if current_page_rid <= self.table.base_RID and current_page_rid != 0: #if this is a base page it will be less than the table base_RID value
                if current_page_indirection != 0: #want to find the latest entry values to store to index
                    page_index, slot_index = self.table.page_directory[current_page_indirection] #update these values to reflect a tail hop
                    current_range = self.table.buffer.fetch_range(self.table.name, page_index)
                    self.table.buffer.unpin_range(self.table.name, page_index)
                    #current_page_rid = current_range[1].read(slot_index)

                for column_index in range(self.table.num_columns):
                    current_value = current_range[column_index + lstore.config.Offset].read(slot_index) #only retrieve the value columns

                    if current_value not in self.index_dict[column_index]: #if there is no entry, create a list entry
                        self.index_dict[column_index][current_value] = [current_page_rid]
                        counter+=1
                    else:
                        if column_index == 0:
                            print("shouldn't be modifying a primary key index, value of rid is " + str(current_page_rid) + " value of key is " + str(current_value))
                        self.index_dict[column_index][current_value].append(current_page_rid) #add to the list entry
                        counter+=1
        #print("create index created an index with " + str(counter/self.table.num_columns) + " elements")
        #print(len(self.index_dict[0]))


    def add_index(self, RID_entry, cols):

        for column_index in range(len(cols)):
            if column_index == self.table.key: # Check for duplicate primary
                if cols[column_index] in self.index_dict[column_index]:
                    if len(self.index_dict[column_index][cols[column_index]]) != 0:
                        return -1

            if cols[column_index] not in self.index_dict[column_index]: #if there is no entry, create a list entry
                self.index_dict[column_index][cols[column_index]] = [RID_entry]
            else:
                self.index_dict[column_index][cols[column_index]].append(RID_entry) #add to the list entry
        return 0

    def update_index(self, RID_entry, cols): #drop the index and add the updated record
        self.drop_index(cols[self.table.key])
        self.add_index(RID_entry, cols)
        value = self.locate(cols[self.table.key], 0)

    # Drop index of specific column
    # Delete given column_number
    def drop_index(self, key):
        # print("drop_index")
        rid = self.locate(key, self.table.key)[0]
        for i in range(self.table.num_columns):
            for key in self.index_dict[i].keys():
                if rid in self.index_dict[i][key]:
                    self.index_dict[i][key].remove(rid)
        return

    # Function to add RIDS from a certain range
    def range(self, start, end, column):
        RIDS = []
        for i in range(start, end + 1):
            RID += self.locate(i, column)
