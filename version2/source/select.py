
from version2.source.heapfile import HeapFile
from version2.source.buffer_pool import BufferPool
from version2.source.type import IntType, StringType
from version2.source.field import FieldDesc
from version2.unittest.util import TestUtil




def test():
    TestUtil.create_catalog()
    buffer_pool = BufferPool()
    table_name = '/home/latin/code/python/latin_database/version2/data/test'
    tuples_case = TestUtil.get_tuples_case(table_name)
    for t in tuples_case:
        buffer_pool.insert_tuple(t)

    table_name = '/home/latin/code/python/latin_database/version2/data/test'
    select(table_name, 'id', 1)

# select * where a1 = a2

def select(table_name, field_name, field_value):
    hf = HeapFile(table_name)
    n = hf.page_num
    for i in range(n):
        tuples = hf.read_page(i)
    t = select_tuple(tuples, field_name, field_value)
    for f in t.fields:
        if f.get_name() == field_name:
            assert(f.get_value() == 1)
            return t


def select_tuple(tuples, field_name, field_value):
    for t in tuples:
        fields = t.fields
        for f in fields:
            if f.get_name() == field_name:
                if f.get_value() == field_value:
                    return t

test()
