from version2.source.tuple import Tuple
from version2.source.tuple_desc import TupleDesc
from version2.source.field import FieldDesc, Field
from version2.source.catalog import Catalog
from version2.source.buffer_pool import BufferPool
from version2.source.type import IntType, StringType
from version2.source.util import *
import os

class TestUtil():
    @staticmethod
    def init_buffer():
        TestUtil.create_catalog()
        buffer_pool = BufferPool()
        table_name1 = '/home/latin/code/python/latin_database/version2/data/test'
        table_name2 = '/home/latin/code/python/latin_database/version2/data/test2'
        tuples_case = TestUtil.get_tuples_case(table_name1)
        Catalog.add_table(table_name1)
        Catalog.add_table(table_name2)
        for t in tuples_case:
            buffer_pool.insert_tuple(table_name1, t)
        return buffer_pool

    @staticmethod
    def create_catalog():
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

    @staticmethod
    def get_field_descs_case():
        name1 = 'id'
        name2 = 'name'
        type1 = IntType()
        type2 = StringType()
        fd1 = FieldDesc(name1, type1)
        fd2 = FieldDesc(name2, type2)
        fds = [fd1, fd2]
        return fds

    @staticmethod
    def get_field_descs_case2():
        name1 = 'no'
        name2 = 'salary'
        type1 = IntType()
        type2 = StringType()
        fd1 = FieldDesc(name1, type1)
        fd2 = FieldDesc(name2, type2)
        fds = [fd1, fd2]
        return fds

    @staticmethod
    def get_tuple_desc_case1():
        fds = TestUtil.get_field_descs_case()
        s = TupleDesc(fds)
        return s

    @staticmethod
    def get_tuple_desc_case2():
        fds = TestUtil.get_field_descs_case2()
        s = TupleDesc(fds)
        return s

    @staticmethod
    def get_tuples_case(table_name):
        id1 = 1
        id2 = 2
        name1 = 'a1'
        name2 = 'a2'
        row1 = [id1, name1]
        row2 = [id2, name2]
        rows = [row1, row2]

        s = TestUtil.get_tuple_desc(table_name)
        n = s.get_field_num()
        tuples = []
        field_descs = s.field_descs
        for row in rows:
            fvs = []
            for i in range(n):
                v = row[i]
                fd = field_descs[i]
                fv = Field(fd, v)
                fvs.append(fv)
            t = Tuple(fvs)
            tuples.append(t)
        return tuples

    @staticmethod
    def get_tuple_desc(table_name):
        return Catalog.get_tuple_desc(table_name)

    @staticmethod
    def clear():
        catalog_file = '/home/latin/code/python/latin_database/version2/data/catalog'
        data_file = '/home/latin/code/python/latin_database/version2/data/test'
        os.remove(catalog_file)
        os.remove(data_file)

    @staticmethod
    def gen_field(field_type, field_name, field_value):
        d = FieldDesc(field_name, field_type)
        f = Field(d, field_value)
        return f
