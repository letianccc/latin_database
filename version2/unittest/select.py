from version2.source.heapfile import HeapFile
from version2.source.buffer_pool import BufferPool
from version2.source.type import IntType, StringType
from version2.source.field import FieldDesc
from version2.unittest.util import TestUtil
import os


def test_select():
    table_name = '/home/latin/code/python/latin_database/version2/data/test'
    buffer_pool = TestUtil.init_buffer()

    target_field_names = ['name']
    filter_fields = dict()
    filter_fields['id'] = 1
    tuples = buffer_pool.select_tuples(table_name, target_field_names, filter_fields)

    name_case = 'name'
    value_case = 'a1'
    type_ = StringType()
    field_case = TestUtil.gen_field(type_, name_case, value_case)
    assert(len(tuples) == 1)
    for t in tuples:
        fields = t.fields
        assert(len(fields) == 1)
        for f in fields:
            assert(f.equal(field_case))
