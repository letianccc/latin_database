

from version3.unittest.util import *
from version3.source.insert import Insert
from version3.source.database import Database
from version3.source.select import Select

import os
import unittest

class TestTransaction(unittest.TestCase):
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

    def tearDown(self):
        os.remove(self.catalog_file)
        os.remove(self.table_name1)
        os.remove(self.table_name2)
        Catalog.clear()
        Database.clear()

    def test_transaction(self):
        tuples_case = TestUtil.get_tuples_case(self.table_name1)
        for t in tuples_case:
            tran_id = 0
            operator = Insert(self.table_name1, t, tran_id)
            operator.execute()
        Database.get_buffer().clear_buffer()

        hf = Catalog.name_to_file(self.table_name1)
        tuples = hf.get_all_tuples()
        assert(len(tuples) == 2)
        for i in range(len(tuples)):
            tuple1 = tuples_case[i]
            tuple2 = tuples[i]
            assert(tuple1.equal(tuple2))
