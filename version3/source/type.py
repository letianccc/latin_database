import struct
import pickle


class Type:
    def equal(self, other):
        if self.get_value() == other.get_value():
            if self.get_size() == other.get_size():
                return True
        return False

class IntType(Type):
    int_byte = pickle.dumps(struct.pack('i', 0))
    size = len(int_byte)
    assert(size == 11)
    # print(size)   11
    # size = 11

    def __init__(self):
        INT = 0
        self.value = INT

    def get_value(self):
        return self.value

    @classmethod
    def get_size(cls):
        return cls.size

class StringType(Type):
    def __init__(self, max_len=20):
        STR = 1
        string_byte = pickle.dumps('')
        self.size = max_len + len(string_byte) + IntType.get_size()
        self.value = STR

    def get_value(self):
        return self.value

    def get_size(self):
        return self.size
