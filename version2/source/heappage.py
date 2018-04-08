import struct
import pickle
from version2.source.type import *
from version2.source.catalog import Catalog

class HeapPage:
    def __init__(self, table_id, page_id):
        self.dirty = False
        self.table_id = table_id
        self.id = page_id
        self.tuple_capacity = self.get_tuple_capacity()
        self.tuples = []

    def get_heapfile(self):
        tid = self.table_id
        hf = Catalog.id_to_file(tid)
        return hf

    def get_tuple_capacity(self):
        PGSIZE = 4096
        tuple_size = Catalog.get_tuple_desc_size(self.table_id)
        size = PGSIZE - IntType.get_size()
        tuple_num = int(size / tuple_size)
        return tuple_num

    def insert_tuple(self, tuple_):
        self.tuples.append(tuple_)
        self.mark_dirty()

    def mark_dirty(self):
        self.dirty = True

    def mark_clear(self):
        self.dirty = False

    def is_dirty(self):
        return self.dirty

    def has_empty_entry(self):
        return len(self.tuples) < self.tuple_capacity
