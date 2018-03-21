from version3.unittest.util import *
from version3.source.insert import Insert
from version3.source.database import Database
from version3.source.select import Select

import os
import unittest

class TestLock(unittest.TestCase):
    def setUp(self):
        self.catalog_file = '/home/latin/code/python/latin_database/version3/data/catalog'
        self.table_name1 = '/home/latin/code/python/latin_database/version3/data/test1'
        self.table_name2 = '/home/latin/code/python/latin_database/version3/data/test2'
        self.tables = [self.table_name1, self.table_name2]
        open(self.table_name1, 'w+').close()
        open(self.table_name2, 'w+').close()
        open(self.catalog_file, 'w+').close()
        self.td1 = TestUtil.gen_tuple_desc1()
        self.td2 = TestUtil.gen_tuple_desc1()
        self.tuple_descs = [self.td1, self.td2]
        Catalog.add_table(self.table_name1, self.td1)
        Catalog.add_table(self.table_name2, self.td2)
        TestUtil.init_data()

    def tearDown(self):
        os.remove(self.catalog_file)
        os.remove(self.table_name1)
        os.remove(self.table_name2)
        Catalog.clear()
        Database.clear()

    def test_acquire_lock(self):
        tran_id = 3
        table_id = Catalog.name_to_id(self.table_name1)
        page_index = 0
        page_id = PageId(table_id, page_index)
        lock_type = 'S'
        buffer_ = Database.get_buffer()
        lm = buffer_.get_lock_manager()
        lm.acquire_lock(tran_id, page_id, lock_type)

        lock = lm.get_lock(page_id)
        holders = lock.holders
        assert(len(holders) == 1)
        assert(holders[0] == tran_id)
        assert(lock.get_type() == lock_type)

    def test_same_tran_acquire_S(self):
        tran_id1 = 3
        table_id1 = Catalog.name_to_id(self.table_name1)
        page_index1 = 0
        page_id1 = PageId(table_id1, page_index1)
        lock_type1 = 'S'
        buffer_ = Database.get_buffer()
        p1 = buffer_.get_page(page_id1, tran_id1, lock_type1)
        p2 = buffer_.get_page(page_id1, tran_id1, lock_type1)

        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id1)
        holders = lock.holders
        assert(len(holders) == 1)
        assert(holders[0] == tran_id1)
        assert(lock.get_type() == lock_type1)
        assert(p1.dirty == p2.dirty)
        assert(p1.capacity == p2.capacity)
        assert(p1.get_id().equal(p2.get_id()))
        assert(len(p1.tuples) == len(p2.tuples))

    def test_different_tran_acquire_S(self):
        tran_id1 = 3
        table_id1 = Catalog.name_to_id(self.table_name1)
        page_index1 = 0
        page_id1 = PageId(table_id1, page_index1)
        lock_type1 = 'S'
        buffer_ = Database.get_buffer()
        p1 = buffer_.get_page(page_id1, tran_id1, lock_type1)

        tran_id2 = 4
        table_id2 = Catalog.name_to_id(self.table_name1)
        page_index2 = 0
        page_id2 = PageId(table_id2, page_index2)
        lock_type2 = 'S'
        buffer_ = Database.get_buffer()
        p2 = buffer_.get_page(page_id2, tran_id2, lock_type2)

        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id1)
        holders = lock.holders
        assert(len(holders) == 2)
        assert(holders[0] == tran_id1)
        assert(holders[1] == tran_id2)
        assert(lock.get_type() == lock_type1)
        assert(p1.dirty == p2.dirty)
        assert(p1.capacity == p2.capacity)
        assert(p1.get_id().equal(p2.get_id()))
        assert(len(p1.tuples) == len(p2.tuples))

    
