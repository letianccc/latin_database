from version3.unittest.util import *
from version3.source.insert import Insert
from version3.source.database import Database
from version3.source.select import Select
from multiprocessing.dummy import Pool
from threading import Thread
import time


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
        page_id = TestUtil.gen_page_id1()
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
        page_id = TestUtil.gen_page_id1()
        lock_type1 = 'S'
        buffer_ = Database.get_buffer()
        p1 = buffer_.get_page(page_id, tran_id1, lock_type1)
        p2 = buffer_.get_page(page_id, tran_id1, lock_type1)

        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id)
        holders = lock.holders
        assert(len(holders) == 1)
        assert(holders[0] == tran_id1)
        assert(lock.get_type() == lock_type1)
        assert(p1.dirty == p2.dirty)
        assert(p1.capacity == p2.capacity)
        assert(p1.get_id().equal(p2.get_id()))
        assert(len(p1.tuples) == len(p2.tuples))

    def test_different_tran_acquire_S(self):
        tran_id1 = 1
        tran_id2 = 2
        page_id = TestUtil.gen_page_id1()
        lock_type1 = 'S'
        lock_type2 = 'S'
        array = [(page_id, tran_id1, lock_type1),
                 (page_id, tran_id2, lock_type2)
                 ]
        buffer_ = Database.get_buffer()
        with Pool() as pool:
            p1, p2 = pool.starmap(buffer_.get_page, array)

        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id)
        holders = lock.holders
        assert(len(holders) == 2)
        assert(holders[0] == tran_id1)
        assert(holders[1] == tran_id2)
        assert(lock.get_type() == lock_type1)
        assert(p1.dirty == p2.dirty)
        assert(p1.capacity == p2.capacity)
        assert(p1.get_id().equal(p2.get_id()))
        assert(len(p1.tuples) == len(p2.tuples))

    def test_waitlist_2_tran(self):
        tran_id1 = 1
        tran_id2 = 2
        trans = [tran_id1, tran_id2]
        page_id = TestUtil.gen_page_id1()
        lock_type1 = 'S'
        lock_type2 = 'X'
        array = [(page_id, tran_id1, lock_type1),
                 (page_id, tran_id2, lock_type2)
                 ]
        buffer_ = Database.get_buffer()
        thread1 = Thread(target = buffer_.get_page, args = array[0])
        thread2 = Thread(target = buffer_.get_page, args = array[1])
        thread2.daemon = True
        t1 = time.time()
        thread1.start()
        thread1.join()
        t2 = time.time()
        exec_time = t2 - t1 + 0.05
        thread2.start()
        wait_time = exec_time * (len(trans) - 1)
        time.sleep(wait_time)

        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id)
        wl = lm.waitlist
        assert (thread1.is_alive() == False)
        assert (thread2.is_alive() == True)
        assert(tran_id2 in wl)
        assert(len(lock.holders) == 1)
        assert(lock.holders[0] == tran_id1)
        assert(lock.get_type() == lock_type1)

    def test_waitlist_more_tran(self):
        tran_id1 = 1
        tran_id2 = 2
        tran_id3 = 3
        trans = [tran_id1, tran_id2, tran_id3]
        page_id = TestUtil.gen_page_id1()
        lock_type1 = 'S'
        lock_type2 = 'X'
        lock_type3 = 'X'
        lock_types = [lock_type1, lock_type2, lock_type3]
        args_list = []
        for i in range(len(trans)):
            tran_id = trans[i]
            type_ = lock_types[i]
            args = (page_id, tran_id, type_)
            args_list.append(args)
        buffer_ = Database.get_buffer()
        threads = []
        for args in args_list:
            t = Thread(target = buffer_.get_page, args = args)
            threads.append(t)
        for i in range(1, len(threads)):
            t = threads[i]
            t.daemon = True
        t1 = time.time()
        threads[0].start()
        threads[0].join()
        t2 = time.time()
        exec_time = t2 - t1 + 0.05
        for i in range(1, len(threads)):
            t = threads[i]
            t.start()
        wait_time = exec_time * (len(trans) - 1)
        time.sleep(wait_time)

        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id)
        wl = lm.waitlist
        for i in range(1, len(threads)):
            t = threads[i]
            assert (t.is_alive() == True)
        for i in range(1, len(trans)):
            tran_id = trans[i]
            assert (tran_id in wl)
        assert (threads[0].is_alive() == False)
        assert (trans[0] not in wl)
        assert(len(lock.holders) == 1)
        assert(lock.holders[0] == tran_id1)
        assert(lock.get_type() == lock_type1)
