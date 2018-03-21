# from version3.unittest.util import *
# import os
#
#
# import unittest
#
# class TestCatalog(unittest.TestCase):
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
#
#     def tearDown(self):
#         os.remove(self.catalog_file)
#         os.remove(self.table_name1)
#         os.remove(self.table_name2)
#         Catalog.clear()
#
#     def test_create_table1(self):
#         Catalog.add_table(self.table_name1, self.td1)
#         Catalog.add_table(self.table_name2, self.td2)
#
#         for i in range(len(self.tables)):
#             test_td = self.tuple_descs[i]
#             table_id = i
#             td = Catalog.id_to_tuple_desc(table_id)
#             assert (td.equal(test_td))
#
#     def test_create_table2(self):
#         table_name = '/home/latin/code/python/latin_database/version3/data/test1'
#         fields = dict()
#         field_name = 'id'
#         field_type = IntType()
#         fields[field_name] = field_type
#         field_name = 'name'
#         field_type = StringType()
#         fields[field_name] = field_type
#         field_name = 'dept'
#         field_type = StringType()
#         fields[field_name] = field_type
#         Catalog.create_table(table_name, fields)
#         td = Catalog.get_tuple_desc(table_name)
#         assert(td != None)
#         fds = td.get_field_descs()
#         for fd in fds:
#             n = fd.get_name()
#             t = fd.get_type()
#             t_ = fields[n]
#             assert(t.equal(t_))
