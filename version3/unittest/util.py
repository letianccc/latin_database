from version3.source.tuple import Tuple
from version3.source.tuple_desc import TupleDesc
from version3.source.field import FieldDesc, Field
from version3.source.catalog import Catalog
from version3.source.type import IntType, StringType
from version3.source.heappage import HeapPage, PageId
from version3.source.insert import Insert
from version3.source.database import Database
import os

class TestUtil():
    @staticmethod
    def file_init(cls):
        table_name1 = '/home/latin/code/python/latin_database/version3/data/test1'
        table_name2 = '/home/latin/code/python/latin_database/version3/data/test2'
        catalog_file = '/home/latin/code/python/latin_database/version3/data/catalog'
        open(table_name1, 'w+').close()
        open(table_name2, 'w+').close()
        open(catalog_file, 'w+').close()


    @staticmethod
    def create_catalog():
        table_name1 = '/home/latin/code/python/latin_database/version3/data/test1'
        table_name2 = '/home/latin/code/python/latin_database/version3/data/test2'
        td1 = TestUtil.gen_tuple_desc1()
        td2 = TestUtil.gen_tuple_desc1()
        Catalog.add_table(table_name1, td1)
        Catalog.add_table(table_name2, td2)


    @staticmethod
    def init_data():
        page_num = 3
        table_name1 = '/home/latin/code/python/latin_database/version3/data/test1'
        tuples_case = TestUtil.gen_tuples(table_name1, page_num)
        for t in tuples_case:
            operator = Insert(table_name1, t)
            operator.execute()
        Database.get_buffer().clear_buffer()

    @staticmethod
    def gen_tuples(table_name, page_num):
        pages = TestUtil.gen_pages(table_name, page_num)
        tuples = []
        for p in pages:
            tuples += p.get_tuples()
        return tuples

    @staticmethod
    def gen_pages(table_name, page_num):
        pages = []
        for page_index in range(page_num):
            p = TestUtil.gen_page(table_name, page_index)
            pages.append(p)
        return pages

    @staticmethod
    def gen_page(table_name, page_index, tuple_num=None):
        tid = Catalog.name_to_id(table_name)
        pid = PageId(tid, page_index)
        tuples = TestUtil.gen_tuples_for_page1(table_name, page_index, tuple_num)
        p = HeapPage(pid, tuples)
        return p

    @staticmethod
    def gen_tuples_for_page1(table_name, page_index, tuple_num=None):
        tuple_desc = TestUtil.get_tuple_desc(table_name)
        field_descs = tuple_desc.get_field_descs()
        if tuple_num == None:
            tuple_num = Catalog.get_page_capacity(table_name)
        tuples = []
        for i in range(tuple_num):
            id_ = i + page_index * 4096
            value1 = id_
            value2 = 'a' + str(id_)
            values = [value1, value2]
            fields = []
            for i in range(len(field_descs)):
                d = field_descs[i]
                v = values[i]
                f = Field(d, v)
                fields.append(f)
            t = Tuple(fields)
            tuples.append(t)
        return tuples

    @staticmethod
    def gen_field_descs1():
        name1 = 'id'
        name2 = 'name'
        type1 = IntType()
        type2 = StringType()
        fd1 = FieldDesc(name1, type1)
        fd2 = FieldDesc(name2, type2)
        fds = [fd1, fd2]
        return fds

    @staticmethod
    def gen_field_descs2():
        name1 = 'no'
        name2 = 'salary'
        type1 = IntType()
        type2 = StringType()
        fd1 = FieldDesc(name1, type1)
        fd2 = FieldDesc(name2, type2)
        fds = [fd1, fd2]
        return fds

    @staticmethod
    def gen_tuple_desc1():
        fds = TestUtil.gen_field_descs1()
        s = TupleDesc(fds)
        return s

    @staticmethod
    def gen_tuple_desc2():
        fds = TestUtil.gen_field_descs2()
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
        catalog_file = '/home/latin/code/python/latin_database/version3/data/catalog'
        data_file1 = '/home/latin/code/python/latin_database/version3/data/test1'
        data_file2 = '/home/latin/code/python/latin_database/version3/data/test2'
        open(catalog_file, 'wb+').close()
        open(data_file1, 'wb+').close()
        open(data_file2, 'wb+').close()
        os.remove(catalog_file)
        os.remove(data_file1)
        os.remove(data_file2)
        Catalog.clear()
        Database.clear()

    @staticmethod
    def gen_field(field_type, field_name, field_value):
        d = FieldDesc(field_name, field_type)
        f = Field(d, field_value)
        return f

    @staticmethod
    def print_tuple(tuple_):
        fields = tuple_.get_fields()
        for f in fields:
            print(f.get_value(), end=' ')
        print()

class TestLockUtil:
    def __init__(self, tran_id, page_id, lock_type):
        self.acquire = False
        self.acquire_lock(tran_id, page_id, lock_type)


    def acquire_lock(tran_id, page_id, lock_type):
        buffer_ = Database.get_buffer()
        buffer_.get_page(tran_id, page_id, lock_type)
        lm = buffer_.get_lock_manager()
        lock = lm.get_lock(page_id)
        holders = lock.holders
        if len(holders) == 1:
            if holders[0] == tran_id:
                if lock.get_type() == lock_type:
                    self.acquire = True
