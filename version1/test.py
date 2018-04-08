import pickle
import struct
class A:
    def __init__(self, value):
        self.v = V(value)
        self.next = None

class V:
    def __init__(self, value):
        self.v = value
        self.next = None

a1 = A(1)
a2 = A(2)
a3 = A(3)
a1.next = a2
a2.next = a3


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


f = open('test.db', 'r+b')
a1_b = pickle.dumps(a1)
len_ = len(a1_b)
write_integer(f, len_)
f.write(a1_b)
f.flush()
f.seek(0)
len_ = read_integer(f)
a1 = pickle.loads(f.read(len_))
print(a1.v.v, a1.next.v.v, a1.next.next.v.v)