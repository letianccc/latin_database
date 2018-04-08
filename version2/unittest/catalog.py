from version2.unittest.util import *
from version2.source.util import *
import os





def test_catalog():
    table_name1 = '/home/latin/code/python/latin_database/version2/data/test'
    table_name2 = '/home/latin/code/python/latin_database/version2/data/test2'
    table_names = [table_name1, table_name2]
    open(table_name1, 'w+')
    open(table_name2, 'w+')
    catalog_file = '/home/latin/code/python/latin_database/version2/data/catalog'
    f = open(catalog_file, 'wb+')
    td1 = TestUtil.get_tuple_desc_case1()
    td2 = TestUtil.get_tuple_desc_case2()
    tuple_descs = [td1, td2]

    table_num = len(table_names)
    write_table_num(f, table_num)
    for i in range(table_num):
        n = table_names[i]
        td = tuple_descs[i]
        write_table_name(f, n)
        write_tuple_desc(f, td)

    f.seek(0)
    table_num_ = read_table_num(f)
    table_names_ = []
    tuple_descs_ = []
    for i in range(table_num_):
        n = read_table_name(f)
        t = read_tuple_desc(f)
        table_names_.append(n)
        tuple_descs_.append(t)

    assert(table_num == table_num_)
    for i in range(table_num):
        n1 = table_names[i]
        n2 = table_names_[i]
        td1 = tuple_descs[i]
        td2 = tuple_descs_[i]
        assert(n1 == n2)
        assert(td1.equal(td2))

    os.remove(catalog_file)
    os.remove(table_name1)
    os.remove(table_name2)


def test_catalog1():
    table_name1 = '/home/latin/code/python/latin_database/version2/data/test'
    table_name2 = '/home/latin/code/python/latin_database/version2/data/test2'
    catalog_file = '/home/latin/code/python/latin_database/version2/data/catalog'
    TestUtil.create_catalog()
    Catalog.add_table(table_name1)
    Catalog.add_table(table_name2)

    table_id = 0
    d = Catalog.id_to_tuple_desc(table_id)
    test_d = TestUtil.get_tuple_desc(table_name1)
    assert(d.equal(test_d))
    table_id = 1
    d = Catalog.id_to_tuple_desc(table_id)
    test_d = TestUtil.get_tuple_desc(table_name2)
    assert (d.equal(test_d))

    os.remove(catalog_file)
    os.remove(table_name1)
    os.remove(table_name2)
