import struct
import pickle


def get_class_name(object_):
    return object_.__class__.__name__

def isInt(type_):
    return get_class_name(type_) == 'IntType'

def integer_to_bytes(integer):
    INTEGER_FORMAT = '!Q'
    byte = struct.pack(INTEGER_FORMAT, integer)
    return byte

def bytes_to_integer(integer_bytes):
    INTEGER_FORMAT = '!Q'
    i = struct.unpack(INTEGER_FORMAT, integer_bytes)[0]
    return i

def integer_to_bytes(integer):
    INTEGER_FORMAT = '!Q'
    byte = struct.pack(INTEGER_FORMAT, integer)
    return byte

def read_integer(f):
    INTEGER_LENGTH = 8
    byte = f.read(INTEGER_LENGTH)
    i = bytes_to_integer(byte)
    return i

def write_integer(f, integer):
    b = integer_to_bytes(integer)
    f.write(b)



def write_string(f, string, max_string_size=None):
    byte = pickle.dumps(string)
    len_ = len(byte)
    write_integer(f, len_)
    f.write(byte)
    if max_string_size:
        pad_size = max_string_size - len_
        write_pad(f, pad_size)

def read_string(f, max_string_size=None):
    len_ = read_integer(f)
    byte = f.read(len_)
    string = pickle.loads(byte)
    if max_string_size:
        pad_size = max_string_size - len_
        skip_pad(f, pad_size)
    return string

def skip_pad(f, pad_size):
    pos = f.tell() + pad_size
    f.seek(pos)



def write_pad(f, pad_num):
    for i in range(pad_num):
        byte = bytes([0])
        f.write(byte)

def insert_tuple(f, tuple_):
    for field in tuple_.fields:
        write_field(f, field)

def write_field(f, field):
    t = field.get_type()
    v = field.get_value()
    if isInt(t):
        write_integer(f, v)
    else:
        max_string_size = t.size
        write_string(f, v, max_string_size)



def write_field_desc(f, field_desc):
    write_string(f, field_desc.get_name())
    t = field_desc.get_type()
    v = t.get_value()
    write_integer(f, v)

def read_field_desc(f):
    from version2.source.type import IntType, StringType
    from version2.source.field import FieldDesc
    INT = 0
    STR = 1
    name = read_string(f)
    type_ = read_integer(f)
    if type_ == INT:
        t = IntType()
    elif type_ == STR:
        t = StringType()
    fd = FieldDesc(name, t)
    return fd

def write_table_num(f, table_num):
    write_integer(f, table_num)

def read_table_num(f):
    return read_integer(f)

def write_table_name(f, table_name):
    write_string(f, table_name)

def read_table_name(f):
    return read_string(f)

def write_field_num(f, field_num):
    write_integer(f, field_num)

def read_field_num(f):
    return read_integer(f)

def write_tuple_desc(f, tuple_desc):
    n = tuple_desc.get_field_num()
    write_field_num(f, n)
    fds = tuple_desc.field_descs
    for fd in fds:
        write_field_desc(f, fd)

def read_tuple_desc(f):
    from version2.source.tuple_desc import TupleDesc
    n = read_field_num(f)
    field_descs = []
    for i in range(n):
        fd = read_field_desc(f)
        field_descs.append(fd)
    td = TupleDesc(field_descs)
    return td
