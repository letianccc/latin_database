from version3.source.type import *
from version3.source.field import *
from version3.source.util import *
from version3.source.tuple import Tuple, TupleId
from version3.source.catalog import Catalog


class FileManager():
    PAGESIZE = 4096

    @classmethod
    def get_file_handle(cls, table_name):
        file_name = table_name
        f = open(file_name, 'rb+')
        return f

    @classmethod
    def flush_page(cls, page):
        tid = page.get_table_id()
        index = page.get_index()
        table_name = Catalog.id_to_name(tid)
        with open(table_name, 'rb+') as f:
            tuples = page.tuples
            cls.seek_page(f, index)
            n = len(tuples)
            cls.write_tuple_num(f, n)
            for t in tuples:
                cls.write_tuple(f, t)

    @classmethod
    def seek_page(cls, f, page_id):
        pos = IntType.get_size()
        pos += page_id * cls.PAGESIZE
        f.seek(pos)

    @classmethod
    def flush_page_num(cls, table_id, page_num):
        table_name = Catalog.id_to_name(table_id)
        with open(table_name, 'rb+') as f:
            cls.write_integer(f, page_num)

    @classmethod
    def write_tuple_num(cls, f, tuple_num):
        cls.write_integer(f, tuple_num)

    @classmethod
    def write_tuple(cls, f, tuple_):
        fields = tuple_.fields
        for field in fields:
            cls.write_field(f, field)

    @classmethod
    def write_field(cls, f, field):
        t = field.get_type()
        v = field.get_value()
        if isInt(t):
            cls.write_integer(f, v)
        else:
            max_string_size = t.size
            cls.write_string(f, v, max_string_size)

    @classmethod
    def write_integer(cls, f, integer):
        write_integer(f, integer)

    @classmethod
    def write_string(cls, f, string, max_string_size):
        write_string(f, string, max_string_size)

    @classmethod
    def read_page(cls, page_id):
        tid = page_id.get_table_id()
        table_name = Catalog.id_to_name(tid)
        with open(table_name, 'rb+') as f:
            index = page_id.get_page_index()
            cls.seek_page(f, index)
            desc = Catalog.id_to_tuple_desc(tid)
            n = cls.read_tuple_num(f)
            tuples = []
            for i in range(n):
                t = cls.read_tuple(f, desc, page_id)
                tuples.append(t)
            return tuples

    @classmethod
    def read_tuple(cls, f, scheme, page_id):
        fields = []
        field_descs = scheme.field_descs
        pos = f.tell()
        for fd in field_descs:
            t = fd.get_type()
            if isInt(t):
                field_value = cls.read_integer(f)
            else:
                max_string_size = t.size
                field_value = cls.read_string(f, max_string_size)
            field = Field(fd, field_value)
            fields.append(field)
        tid = TupleId(page_id, pos)
        t = Tuple(fields, tid)
        return t

    @classmethod
    def read_tuple_num(cls, f):
        n = cls.read_integer(f)
        if n == None:
            n = 0
        return n

    @classmethod
    def read_integer(cls, f):
        i = read_integer(f)
        return i

    @classmethod
    def read_string(cls, f, max_string_size):
        return read_string(f, max_string_size)
