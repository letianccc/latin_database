from version2.source.util import *
from version2.source.file_manager import File_Manager
import os


class Catalog:
    # __id_to_name = dict()
    __id_to_file = dict()
    __id_to_tuple_desc = dict()
    __name_to_file = dict()
    # __name_to_id = dict()

    table_id = 0

    @classmethod
    def add_table(cls, table_name):
        tid = cls.table_id
        desc = cls.set_id_to_tuple_desc(tid, table_name)
        hf = cls.set_id_to_file(tid, table_name, desc)
        cls.set_name_to_file(table_name, hf)
        cls.increase_table_id()

    @classmethod
    def get_page_num(cls, table_name):
        if os.stat(table_name).st_size == 0:
            return 0
        with open(table_name, 'rb+') as f:

            page_num = read_integer(f)
            return page_num

    @classmethod
    def increase_table_id(cls):
        cls.table_id += 1

    @classmethod
    def set_id_to_file(cls, table_id, table_name, tuple_desc):
        from version2.source.heapfile import HeapFile
        fm = File_Manager(table_id, table_name, tuple_desc)
        page_num = cls.get_page_num(table_name)
        hf = HeapFile(table_id, fm, page_num)
        cls.__id_to_file[table_id] = hf
        return hf

    @classmethod
    def set_id_to_tuple_desc(cls, table_id, table_name):
        desc = cls.get_tuple_desc(table_name)
        cls.__id_to_tuple_desc[table_id] = desc
        return desc

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
    def is_valid_table(cls, table_name):
        exist_table = cls.__name_to_file.__contains__(table_name)
        return exist_table

    @classmethod
    def get_tuple_desc(cls, target_table_name):
        catalog_file = '/home/latin/code/python/latin_database/version2/data/catalog'
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
