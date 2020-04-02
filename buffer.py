from lstore.table import *
from lstore.db import *
from lstore.page import Page
import lstore.config
import lstore.table
import math
import os
import threading

#page_map: contains a list of base or tail ranges
class Bufferpool():
    def __init__(self, db):
        self.db = db
        self.frame_map = {} # page slot number to frame id
        self.page_map = {}  # frame id to page
        self.size = lstore.config.buffersize
        self.accesses = [0] * self.size
        self.pins = [0] * self.size #TODO: for milestone 3, make pins an int

    def must_evict(self):
        return len(self.page_map) == self.size

    #fetches the range and returns, while putting it in a frame index
    def fetch_range(self, name, page_slot):
        if page_slot in self.frame_map: #if in memory, just return
            self.accesses[self.frame_map[page_slot]] += 1
            self.pin_range(name, page_slot) #pin this page
            return self.page_map[self.frame_map[page_slot]]
        else:
            new_range = self.get_range(name, page_slot)
            if self.must_evict(): #must evict a page and store a new one from disk
                frame_num = self.evict(name)
                self.frame_map[page_slot] = frame_num
                self.page_map[frame_num] = new_range
            else: #there is space in the buffer pool to fit a new set of ranges
                self.frame_map[page_slot] = len(self.page_map)
                self.page_map[self.frame_map[page_slot]] = new_range


        self.accesses[self.frame_map[page_slot]] += 1 #increase num accesses for this frame
        self.pin_range(name, page_slot) #pin this page
        return self.page_map[self.frame_map[page_slot]]

    #check to see if the range is pinned
    def is_pinned(self, name, page_slot):
        frame_num = self.frame_map[page_slot]
        return self.pins[frame_num] > 0

    #TODO: for milestone 3, make pins an int
    def pin_range(self, name, page_slot):
        frame_num = self.frame_map[page_slot]
        self.pins[frame_num] += 1

    def get_pins(self, name, page_slot):
        return self.pins[self.frame_map[page_slot]]

    #TODO: for milestone 3, make pins an int
    def unpin_range(self, name, page_slot):
        frame_num = self.frame_map[page_slot]
        self.pins[frame_num] -= 1

    #get the specified page_index
    def get_range(self, name, page_index):
        curr_table = self.db.get_table(name)
        new_range = []
        for column_index in range(lstore.config.Offset + curr_table.num_columns):
            new_page = curr_table.disk.fetch_page(name, column_index, page_index) #fetch page from disk
            new_range.append(new_page)

        return new_range

    def add_range(self, name, page_slot):
        curr_table = self.db.get_table(name)
        new_range = []

        for column_index in range(lstore.config.Offset + curr_table.num_columns):
            new_page = Page()
            new_range.append(new_page)

        if self.must_evict(): #need to evict a page to add the new range from memory
            frame_num = self.evict(name)
            self.frame_map[page_slot] = frame_num
            self.page_map[frame_num]= new_range
            self.accesses[frame_num] += 1 #increase num accesses for this frame
        else: #space in the buffer pool to add a new range from memory
            self.frame_map[page_slot] = len(self.page_map)
            self.page_map[self.frame_map[page_slot]] = new_range
            self.accesses[self.frame_map[page_slot]] += 1 #increase num accesses for this frame

    def evict(self, name):
        count = math.inf
        evict_page_slot = None
        for fk in self.frame_map.keys():
            frame_num = self.frame_map[fk]
            num_accesses = self.accesses[frame_num]
            num_pins = self.pins[frame_num]
            if num_accesses < count and num_pins == 0:
                count = num_accesses
                evict_page_slot = fk

        curr_table = self.db.get_table(name)
        for column_index in range(lstore.config.Offset + curr_table.num_columns):
            page_to_write = self.page_map[self.frame_map[evict_page_slot]][column_index]
            curr_table.disk.write(name, column_index, evict_page_slot, page_to_write)

        evicted_key = self.frame_map[evict_page_slot]
        del self.frame_map[evict_page_slot] #need to remove the key from the map to prevent an access from happening again
        return evicted_key
