import struct
import pickle
from version3.source.type import *
from version3.source.catalog import Catalog

class HeapPage:
    def __init__(self, page_id, tuples):
        self.dirty = False
        self.id = page_id
        self.capacity = self.get_capacity()
        self.tuples = tuples

    def insert_tuple(self, tuple_, tran_id=None):
        self.tuples.append(tuple_)
        self.mark_dirty()

    def delete_tuple(self, tuple_):
        self.tuples.remove(tuple_)
        self.mark_dirty()

    def get_match_tuples(self, filt_fields):
        tuples = []
        for t in self.tuples:
            if t.is_match(filt_fields):
                tuples.append(t)
        return tuples

    def get_tuples(self):
        return self.tuples

    def get_table_id(self):
        return self.id.get_table_id()

    def get_id(self):
        return self.id

    def get_index(self):
        return self.id.get_page_index()

    def equal_id(self, another_id):
        return self.id.equal(another_id)

    def get_capacity(self):
        PGSIZE = 4096
        tid = self.id.get_table_id()
        tuple_size = Catalog.get_tuple_desc_size(tid)
        size = PGSIZE - IntType.get_size()
        tuple_num = int(size / tuple_size)
        return tuple_num

    def mark_dirty(self):
        self.dirty = True

    def mark_clear(self):
        self.dirty = False

    def is_dirty(self):
        return self.dirty

    def has_empty_entry(self):
        return len(self.tuples) < self.capacity

class PageId:
    def __init__(self, table_id , page_id):
        self.__table_id = table_id
        self.__index = page_id

    def equal(self, another):
        if self.get_table_id() == another.get_table_id():
            if self.get_page_index() == another.get_page_index():
                return True
        return False

    def get_table_id(self):
        return self.__table_id

    def get_page_index(self):
        return self.__index
