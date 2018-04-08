from version2.source.type import *
from version2.source.field import *
from version2.source.util import *
from version2.source.tuple import Tuple



class File_Manager():
    def __init__(self, table_id, table_name, tuple_desc):
        self.tuple_desc = tuple_desc
        self.f = self.get_file_handle(table_name)
        self.id = table_id
        self.PAGESIZE = 4096

    def get_file_handle(self, table_name):
        file_name = table_name
        f = open(file_name, 'rb+')
        return f

    def flush_page(self, page):
        page_id = page.id
        tuples = page.tuples
        self.seek_page(page_id)
        self.write_tuple_num(tuples)
        for t in tuples:
            self.write_tuple(t)
        self.f.seek(0)

    def seek_page(self, page_id):
        pos = IntType.get_size()
        pos += page_id * self.PAGESIZE
        self.f.seek(pos)

    def flush_page_num(self, page_num):
        cur_pos = self.f.tell()
        self.f.seek(0)
        self.write_integer(page_num)
        self.f.seek(cur_pos)

    def write_tuple_num(self, tuples):
        n = len(tuples)
        self.write_integer(n)

    def write_tuple(self, tuple_):
        fields = tuple_.fields
        for f in fields:
            self.write_field(f)

    def write_field(self, field):
        t = field.get_type()
        v = field.get_value()
        if isInt(t):
            self.write_integer(v)
        else:
            max_string_size = t.size
            self.write_string(v, max_string_size)

    def write_integer(self, integer):
        write_integer(self.f, integer)

    def write_string(self, string, max_string_size):
        write_string(self.f, string, max_string_size)

    def read_page(self, page_id):
        tuple_desc = self.tuple_desc
        self.seek_page(page_id)
        n = self.read_tuple_num()
        tuples = []
        for i in range(n):
            t = self.read_tuple(tuple_desc)
            tuples.append(t)
        return tuples

    def read_tuple(self, scheme):
        fields = []
        field_descs = scheme.field_descs
        for fd in field_descs:
            t = fd.get_type()
            if isInt(t):
                field_value = self.read_integer()
            else:
                max_string_size = t.size
                field_value = self.read_string(max_string_size)
            field = Field(fd, field_value)
            fields.append(field)
        t = Tuple(fields)
        return t

    def read_tuple_num(self):
        n = self.read_integer()
        return n

    def read_integer(self):
        i = read_integer(self.f)
        return i

    def read_string(self, max_string_size):
        return read_string(self.f, max_string_size)
