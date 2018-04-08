from version3.source.util import *
from version3.source.field import FieldDesc
from version3.source.tuple_desc import TupleDesc

import os


class Catalog:
    __id_to_name = dict()
    __id_to_file = dict()
    __id_to_tuple_desc = dict()
    __name_to_file = dict()
    # __name_to_id = dict()
    file_name = '/home/latin/code/latin/python/latin_database/version3/data/catalog'


    table_id = 0

    @classmethod
    def create_table(cls, table_name, fields):
        td = Catalog.gen_tuple_desc(fields)
        Catalog.write_table(table_name, td)

        tid = cls.table_id
        cls.set_id_to_name(tid, table_name)
        cls.set_id_to_tuple_desc(tid, td)
        hf = cls.set_id_to_file(tid, table_name, td)
        cls.set_name_to_file(table_name, hf)
        cls.increase_table_id()

    @classmethod
    def add_table(cls, table_name, tuple_desc):
        Catalog.write_table(table_name, tuple_desc)
        tid = cls.table_id
        cls.set_id_to_name(tid, table_name)
        cls.set_id_to_tuple_desc(tid, tuple_desc)
        hf = cls.set_id_to_file(tid, table_name, tuple_desc)
        cls.set_name_to_file(table_name, hf)
        cls.increase_table_id()

    @classmethod
    def write_table(cls, table_name, tuple_desc):
        with open(cls.file_name, 'rb+') as f:
            n = read_table_num(f)
            f.seek(0)
            write_table_num(f, n + 1)
            f.seek(0, 2)
            write_table_name(f, table_name)
            write_tuple_desc(f, tuple_desc)

    @classmethod
    def read_table(cls):
        with open(cls.file_name, 'rb') as f:
            table_num = read_table_num(f)
            table_data = dict()
            for i in range(table_num):
                n = read_table_name(f)
                t = read_tuple_desc(f)
                table_data[n] = t
        return table_data

    @classmethod
    def gen_tuple_desc(cls, fields):
        fds = []
        for name, type_ in fields.items():
            fd = FieldDesc(name, type_)
            fds.append(fd)
        td = TupleDesc(fds)
        return td

    @classmethod
    def get_page_num(cls, table_name):
        if not os.path.exists(table_name) or os.stat(table_name).st_size == 0:
            return 0
        with open(table_name, 'rb+') as f:

            page_num = read_integer(f)
            return page_num

    @classmethod
    def increase_table_id(cls):
        cls.table_id += 1

    @classmethod
    def set_id_to_name(cls, table_id, table_name):
        cls.__id_to_name[table_id] = table_name


    @classmethod
    def set_id_to_file(cls, table_id, table_name, tuple_desc):
        from version3.source.heapfile import HeapFile
        page_num = cls.get_page_num(table_name)
        hf = HeapFile(table_id, page_num)
        cls.__id_to_file[table_id] = hf
        return hf

    @classmethod
    def set_id_to_tuple_desc(cls, table_id, tuple_desc):
        cls.__id_to_tuple_desc[table_id] = tuple_desc

    # @classmethod
    # def set_name_to_id(cls, table_id, table_name):
    #     cls.__name_to_id[table_name] = table_id

    @classmethod
    def set_name_to_file(cls, table_name, heapfile):
        cls.__name_to_file[table_name] = heapfile

    @classmethod
    def id_to_file(cls, table_id):
        hf = cls.__id_to_file[table_id]
        return hf

    @classmethod
    def id_to_tuple_desc(cls, table_id):
        hf = cls.__id_to_tuple_desc[table_id]
        return hf

    @classmethod
    def name_to_file(cls, table_name):
        hf = cls.__name_to_file[table_name]
        return hf

    @classmethod
    def id_to_name(cls, table_id):
        table_name = cls.__id_to_name[table_id]
        return table_name

    @classmethod
    def name_to_id(cls, table_name):
        for id_, name in cls.__id_to_name.items():
            if name == table_name:
                return id_


    @classmethod
    def is_valid_table(cls, table_name):
        exist_table = cls.__name_to_file.__contains__(table_name)
        return exist_table

    @classmethod
    def get_tuple_desc(cls, target_table_name):
        catalog_file = '/home/latin/code/latin/python/latin_database/version3/data/catalog'
        with open(catalog_file, 'rb') as f:
            table_num_ = read_table_num(f)
            for i in range(table_num_):
                n = read_table_name(f)
                t = read_tuple_desc(f)
                if n == target_table_name:
                    return t

    @classmethod
    def get_tuple_desc_size(cls, table_id):
        desc = Catalog.id_to_tuple_desc(table_id)
        s = desc.get_size()
        return s

    @classmethod
    def get_page_capacity(cls, table_name):
        from version3.source.type import IntType
        PGSIZE = 4096
        tid = Catalog.name_to_id(table_name)
        tuple_size = Catalog.get_tuple_desc_size(tid)
        size = PGSIZE - IntType.get_size()
        tuple_num = int(size / tuple_size)
        return tuple_num

    @classmethod
    def clear(cls):
        cls.__id_to_name = dict()
        cls.__id_to_file = dict()
        cls.__id_to_tuple_desc = dict()
        cls.__name_to_file = dict()
        cls.table_id = 0
