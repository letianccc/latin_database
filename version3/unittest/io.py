# from version3.unittest.util_io import *
#
# import os
#
# import unittest
#
# class TestIO(unittest.TestCase):
#     def test_read_none(self):
#         table_name = '/home/latin/code/latin/python/latin_database/version3/data/test1'
#         with open(table_name, 'wb+') as f:
#             i = read_integer(f)
#             assert(i == None)
#
#     def test_write_integer(self):
#         table_name = '/home/latin/code/latin/python/latin_database/version3/data/test1'
#         f = open(table_name, 'wb+')
#
#         test_integer = 1
#         write_integer(f, test_integer)
#         f.seek(0)
#         integer = read_integer(f)
#         assert(integer == test_integer)
#
#         f.close()
#         os.remove(table_name)
#
#     def test_write_string(self):
#         table_name = '/home/latin/code/latin/python/latin_database/version3/data/test1'
#         f = open(table_name, 'wb+')
#
#         test_string = 'aaa'
#         write_string(f, test_string)
#         f.seek(0)
#         string = read_string(f)
#         assert(string == test_string)
#
#         f.close()
#         os.remove(table_name)
#
#     def test_write_field(self):
#         from version3.source.type import IntType
#         from version3.source.field import FieldDesc
#
#         name = 'id'
#         type_ = IntType()
#         fd_case = FieldDesc(name, type_)
#
#         table_name = '/home/latin/code/latin/python/latin_database/version3/data/test1'
#         f = open(table_name, 'wb+')
#         write_field_desc(f, fd_case)
#         f.seek(0)
#         fd = read_field_desc(f)
#         assert(fd.equal(fd_case))
#         f.close()
#         os.remove(table_name)
