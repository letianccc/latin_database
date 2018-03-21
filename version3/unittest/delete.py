#
#
# from version3.unittest.util import *
# from version3.source.delete import Delete
# from version3.source.select import Select
#
# from version3.source.database import Database
# import os
# import unittest
#
# class TestDelete(unittest.TestCase):
#     def setUp(self):
#         self.catalog_file = '/home/latin/code/python/latin_database/version3/data/catalog'
#         self.table_name1 = '/home/latin/code/python/latin_database/version3/data/test1'
#         self.table_name2 = '/home/latin/code/python/latin_database/version3/data/test2'
#         self.tables = [self.table_name1, self.table_name2]
#         open(self.table_name1, 'w+').close()
#         open(self.table_name2, 'w+').close()
#         open(self.catalog_file, 'w+').close()
#         self.td1 = TestUtil.gen_tuple_desc1()
#         self.td2 = TestUtil.gen_tuple_desc1()
#         self.tuple_descs = [self.td1, self.td2]
#         Catalog.add_table(self.table_name1, self.td1)
#         Catalog.add_table(self.table_name2, self.td2)
#         TestUtil.init_data()
#
#     def tearDown(self):
#         os.remove(self.catalog_file)
#         os.remove(self.table_name1)
#         os.remove(self.table_name2)
#         Catalog.clear()
#         Database.clear()
#
#     def test_delete(self):
#         filter_fields = dict()
#         filter_name = 'id'
#         filter_value = 1
#         filter_fields[filter_name] = filter_value
#         operator = Delete(self.table_name1, filter_fields)
#         operator.execute()
#         Database.get_buffer().clear_buffer()
#         operator = Select(self.table_name1, filter_fields)
#         tuples = operator.execute()
#         assert(len(tuples) == 0)
