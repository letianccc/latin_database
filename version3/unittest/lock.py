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
        self.catalog_file = '/home/latin/code/latin/python/latin_database/version3/data/catalog'
        self.table_name1 = '/home/latin/code/latin/python/latin_database/version3/data/test1'
        self.table_name2 = '/home/latin/code/latin/python/latin_database/version3/data/test2'
        self.tables = [self.table_name1, self.table_name2]
        open(self.table_name1, 'w+').close()
        open(self.table_name2, 'w+').close()
        open(self.catalog_file, 'w+').close()
        self.td1 = TestUtil.gen_tuple_desc1()
        self.td2 = TestUtil.gen_tuple_desc1()
        self.tuple_descs = [self.td1, self.td2]
        Catalog.add_table(self.table_name1, self.td1)
        Catalog.add_table(self.table_name2, self.td2)

    def tearDown(self):
        os.remove(self.catalog_file)
        os.remove(self.table_name1)
        os.remove(self.table_name2)
        Catalog.clear()
        Database.clear()

    def test_acquire_lock(self):
        tran_id = 3
        page_id = TestUtil.gen_page_id1(0)
        lock_type = 'S'
        buffer_ = Database.get_buffer()
        lm = buffer_.get_lock_manager()
        lm.acquire_lock(tran_id, page_id, lock_type)

        locks = lm.get_locks(page_id)
        lock = locks[0]
        assert(len(locks) == 1)
        assert(locks[0].match_holder(tran_id))
        assert(lock.get_type() == lock_type)

    def test_same_tran_acquire_S(self):
        tran_id1 = 3
        page_id = TestUtil.gen_page_id1(0)
        lock_type1 = 'S'
        buffer_ = Database.get_buffer()
        p1 = buffer_.get_page(page_id, tran_id1, lock_type1)
        p2 = buffer_.get_page(page_id, tran_id1, lock_type1)

        lm = buffer_.get_lock_manager()
        locks = lm.get_locks(page_id)
        lock = locks[0]
        assert(len(locks) == 1)
        assert(locks[0].match_holder(tran_id1))
        assert(lock.get_type() == lock_type1)
        assert(p1.dirty == p2.dirty)
        assert(p1.capacity == p2.capacity)
        assert(p1.get_id().equal(p2.get_id()))
        assert(len(p1.tuples) == len(p2.tuples))

    def test_different_tran_acquire_S(self):
        tran_id1 = 1
        tran_id2 = 2
        page_id = TestUtil.gen_page_id1(0)
        lock_type1 = 'S'
        lock_type2 = 'S'
        array = [(page_id, tran_id1, lock_type1),
                 (page_id, tran_id2, lock_type2)
                 ]
        buffer_ = Database.get_buffer()
        with Pool() as pool:
            p1, p2 = pool.starmap(buffer_.get_page, array)

        lm = buffer_.get_lock_manager()
        locks = lm.get_locks(page_id)
        assert (len(locks) == 2)

        assert(locks[0].match_holder(tran_id1))
        assert(locks[1].match_holder(tran_id2))
        assert(locks[0].get_type() == lock_type1)
        assert(locks[1].get_type() == lock_type1)
        assert(p1.dirty == p2.dirty)
        assert(p1.capacity == p2.capacity)
        assert(p1.get_id().equal(p2.get_id()))
        assert(len(p1.tuples) == len(p2.tuples))

    def test_waitlist_2_tran(self):
        page_id = TestUtil.gen_page_id1(0)
        arg1 = [0, page_id, 'S']
        arg2 = [1, page_id, 'X']
        args = [arg1, arg2]
        grabs = []
        for arg in args:
            tran_id = arg[0]
            pid = arg[1]
            lock_type = arg[2]
            g = GrabLock(tran_id, pid, lock_type)
            g.start()
            grabs.append(g)
        wait_time = 1
        time.sleep(wait_time)

        buffer_ = Database.get_buffer()
        lm = buffer_.get_lock_manager()
        locks = lm.get_locks(page_id)
        wl = lm.waitlist
        assert(1 in wl)
        assert(len(locks) == 1)
        assert(locks[0].match_holder(0))
        assert(locks[0].get_type() == 'S')
        assert (grabs[0].success == True)
        for i in range(1, len(grabs)):
            assert (grabs[i].success == False)

    def test_waitlist_more_tran(self):
        page_id = TestUtil.gen_page_id1(0)
        arg1 = [0, page_id, 'S']
        arg2 = [1, page_id, 'X']
        arg3 = [2, page_id, 'X']
        args = [arg1, arg2, arg3]
        grabs = []
        for arg in args:
            tran_id = arg[0]
            pid = arg[1]
            lock_type = arg[2]
            g = GrabLock(tran_id, pid, lock_type)
            g.start()
            grabs.append(g)
        wait_time = 1
        time.sleep(wait_time)

        buffer_ = Database.get_buffer()
        lm = buffer_.get_lock_manager()
        locks = lm.get_locks(page_id)
        wl = lm.waitlist
        assert (0 not in wl)
        assert(len(locks) == 1)
        assert(locks[0].match_holder(0))
        assert(locks[0].get_type() == 'S')
        for i in range(1, len(args)):
            arg = args[i]
            tran_id = arg[0]
            assert (tran_id in wl)
        assert (grabs[0].success == True)
        for i in range(1, len(grabs)):
            assert (grabs[i].success == False)

    def test_deadlock1(self):
        p1 = TestUtil.gen_page_id1(0)
        p2 = TestUtil.gen_page_id2(0)
        g1 = GrabLock(0, p1, 'X')
        g2 = GrabLock(1, p2, 'X')
        g3 = GrabLock(0, p2, 'S')
        g4 = GrabLock(1, p1, 'S')
        grabs = [g1, g2, g3, g4]
        for g in grabs:
            g.start()

        wait_time = 1
        time.sleep(wait_time)
        assert(g1.success == True)
        assert(g2.success == True)
        assert(g3.success == False)
        assert(g4.success == False)
        assert(g1.deadlock == False)
        assert(g2.deadlock == False)
        assert(g3.deadlock == False)
        assert(g4.deadlock == True)

    def test_deadlock2(self):
        p1 = TestUtil.get_page_id(self.table_name1, 0)
        p2 = TestUtil.get_page_id(self.table_name1, 1)
        p3 = TestUtil.get_page_id(self.table_name1, 2)
        g1 = GrabLock(0, p1, 'X')
        g2 = GrabLock(1, p2, 'X')
        g3 = GrabLock(2, p3, 'X')
        g4 = GrabLock(0, p2, 'S')
        g5 = GrabLock(1, p3, 'S')
        g6 = GrabLock(2, p1, 'S')
        grabs = [g1, g2, g3, g4, g5, g6]
        for g in grabs:
            g.start()

        wait_time = 1
        time.sleep(wait_time)
        assert(g1.success == True)
        assert(g2.success == True)
        assert(g3.success == True)
        assert(g4.success == False)
        assert(g5.success == False)
        assert(g6.success == False)
        assert(g1.deadlock == False)
        assert(g2.deadlock == False)
        assert(g3.deadlock == False)
        assert(g4.deadlock == False)
        assert(g5.deadlock == False)
        assert(g6.deadlock == True)
