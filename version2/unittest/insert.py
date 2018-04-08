from version2.unittest.util import *
from version2.source.buffer_pool import *
import os


# def test_insert_tuple_by_buffer():
#     TestUtil.create_catalog()
#     buffer_pool = BufferPool()
#     table_name1 = '/home/latin/code/python/latin_database/version2/data/test'
#     table_name2 = '/home/latin/code/python/latin_database/version2/data/test2'
#     tuples_case = TestUtil.get_tuples_case(table_name1)
#     Catalog.add_table(table_name1)
#     Catalog.add_table(table_name2)
#     for t in tuples_case:
#         buffer_pool.insert_tuple(table_name1, t)
#
#     tuples = buffer_pool.get_all_tuples(table_name1)
#     assert(len(tuples) == 2)
#     for i in range(len(tuples)):
#         tuple1 = tuples_case[i]
#         tuple2 = tuples[i]
#         assert(tuple1.equal(tuple2))
#
#     TestUtil.clear()

def test_insert_tuple_by_buffer1():
    TestUtil.create_catalog()
    buffer_pool = BufferPool()
    table_name1 = '/home/latin/code/python/latin_database/version2/data/test'
    table_name2 = '/home/latin/code/python/latin_database/version2/data/test2'
    tuples_case = TestUtil.get_tuples_case(table_name1)
    Catalog.add_table(table_name1)
    Catalog.add_table(table_name2)
    for t in tuples_case:
        buffer_pool.insert_tuple(table_name1, t)

    tuples = buffer_pool.get_all_tuples(table_name1)
    assert(len(tuples) == 2)
    for i in range(len(tuples)):
        tuple1 = tuples_case[i]
        tuple2 = tuples[i]
        assert(tuple1.equal(tuple2))

    TestUtil.clear()
