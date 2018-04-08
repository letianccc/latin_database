


# 表的头文件包含：域数量、数据起始位置
# 长度、域名、域类型

class TupleDesc:
    def __init__(self, field_descs):
        self.field_descs = field_descs

    def get_size(self):
        size = 0
        for fd in self.field_descs:
            s = fd.get_size()
            size += s
        return size

    def get_field_num(self):
        return len(self.field_descs)

    def equal(self, other):
        if self.get_size() == other.get_size():
            if self.get_field_num() == other.get_field_num():
                if self.field_descs_equal(other):
                    return True
        return False

    def field_descs_equal(self, other):
        n = other.get_field_num()
        for i in range(n):
            fd1 = self.field_descs[i]
            fd2 = other.field_descs[i]
            if not fd1.equal(fd2):
                return False
        return True
