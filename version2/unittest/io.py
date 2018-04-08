from version2.unittest.insert import *
from version2.unittest.util import *
from version2.source.file_manager import *
import os

def test_init_data():
    TestUtil.init_data()


def test_write_integer():
    table_name = '/home/latin/code/python/latin_database/version2/data/test'
    f = open(table_name, 'wb+')

    test_integer = 1
    write_integer(f, test_integer)
    f.seek(0)
    integer = read_integer(f)
    assert(integer == test_integer)

    f.close()
    os.remove(table_name)

def test_write_string():
    table_name = '/home/latin/code/python/latin_database/version2/data/test'
    f = open(table_name, 'wb+')

    test_string = 'aaa'
    write_string(f, test_string)
    f.seek(0)
    string = read_string(f)
    assert(string == test_string)

    f.close()
    os.remove(table_name)

def test_write_field():
    name = 'id'
    type_ = IntType()
    fd_case = FieldDesc(name, type_)

    table_name = '/home/latin/code/python/latin_database/version2/data/test'
    f = open(table_name, 'wb+')
    write_field_desc(f, fd_case)
    f.seek(0)
    fd = read_field_desc(f)
    assert(fd.equal(fd_case))
    f.close()
    os.remove(table_name)
