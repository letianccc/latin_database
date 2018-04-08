# from version3.unittest.util import *
# from version3.source.insert import Insert
# from version3.source.database import Database
# from version3.source.select import Select
#
# import os
# import unittest
#
# class TestInsert(unittest.TestCase):
#     def setUp(self):
#         self.catalog_file = '/home/latin/code/latin/python/latin_database/version3/data/catalog'
#         self.table_name1 = '/home/latin/code/latin/python/latin_database/version3/data/test1'
#         self.table_name2 = '/home/latin/code/latin/python/latin_database/version3/data/test2'
#         self.tables = [self.table_name1, self.table_name2]
#         open(self.table_name1, 'w+').close()
#         open(self.table_name2, 'w+').close()
#         open(self.catalog_file, 'w+').close()
#         self.td1 = TestUtil.gen_tuple_desc1()
#         self.td2 = TestUtil.gen_tuple_desc1()
#         self.tuple_descs = [self.td1, self.td2]
#         Catalog.add_table(self.table_name1, self.td1)
#         Catalog.add_table(self.table_name2, self.td2)
#
#     def tearDown(self):
#         os.remove(self.catalog_file)
#         os.remove(self.table_name1)
#         os.remove(self.table_name2)
#         Catalog.clear()
#         Database.clear()
#
#     def test_insert_page(self):
#         page_num = 1
#         tuples_case = TestUtil.gen_tuples(self.table_name1, page_num)
#         for t in tuples_case:
#             operator = Insert(self.table_name1, t)
#             operator.execute()
#         Database.get_buffer().clear_buffer()
#
#         hf = Catalog.name_to_file(self.table_name1)
#         tuples = hf.get_all_tuples()
#         assert(len(tuples_case) == 78)
#         assert(len(tuples_case) == len(tuples))
#         for i in range(len(tuples_case)):
#             t1 = tuples_case[i]
#             t2 = tuples[i]
#             assert(t1.equal(t2))
#
#     def test_insert_tuple_by_buffer1(self):
#         tuples_case = TestUtil.get_tuples_case(self.table_name1)
#         for t in tuples_case:
#             operator = Insert(self.table_name1, t)
#             operator.execute()
#         Database.get_buffer().clear_buffer()
#
#         hf = Catalog.name_to_file(self.table_name1)
#         tuples = hf.get_all_tuples()
#         assert(len(tuples) == 2)
#         for i in range(len(tuples)):
#             tuple1 = tuples_case[i]
#             tuple2 = tuples[i]
#             assert(tuple1.equal(tuple2))
#
#     # 一个表多个页插入缓冲池
#     def test_insert_tuple_by_buffer2(self):
#         page_num = 3
#         tuples_case = TestUtil.gen_tuples(self.table_name1, page_num)
#         for t in tuples_case:
#             operator = Insert(self.table_name1, t)
#             operator.execute()
#         Database.get_buffer().clear_buffer()
#
#         hf = Catalog.name_to_file(self.table_name1)
#         tuples = hf.get_all_tuples()
#         assert(len(tuples_case) == 234)
#         assert(len(tuples_case) == len(tuples))
#         for i in range(len(tuples_case)):
#             t1 = tuples_case[i]
#             t2 = tuples[i]
#             assert(t1.equal(t2))
#
#     # 多个表多个页插入缓冲池
#     def test_insert_tuple_by_buffer3(self):
#         page_num = 3
#         tuples_case1 = TestUtil.gen_tuples(self.table_name1, page_num)
#         tuples_case2 = TestUtil.gen_tuples(self.table_name2, page_num)
#         hf1 = Catalog.name_to_file(self.table_name1)
#         hf2 = Catalog.name_to_file(self.table_name2)
#         n = min(len(tuples_case1), len(tuples_case2))
#         for i in range(n):
#             t1 = tuples_case1[i]
#             t2 = tuples_case2[i]
#             operator = Insert(self.table_name1, t1)
#             operator.execute()
#             operator = Insert(self.table_name2, t2)
#             operator.execute()
#         Database.get_buffer().clear_buffer()
#
#         tuples1 = hf1.get_all_tuples()
#         tuples2 = hf2.get_all_tuples()
#         assert(len(tuples1) == 234)
#         assert(len(tuples2) == 234)
#         test_sets = [tuples_case1, tuples_case2]
#         result_sets = [tuples1, tuples2]
#         for i in range(len(test_sets)):
#             test_set = test_sets[i]
#             result_set = result_sets[i]
#             for j in range(len(test_set)):
#                 t1 = test_set[i]
#                 t2 = result_set[i]
#                 assert (t1.equal(t2))
