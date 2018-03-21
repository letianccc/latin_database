

class Tuple:
    def __init__(self, fields, tuple_id=None):
        self.fields = fields
        self.__tuple_id = tuple_id

    def equal(self, other):
        n = self.get_field_num()
        for i in range(n):
            f1 = other.fields[i]
            f2 = self.fields[i]
            if not f1.equal(f2):
                return False
        return True

    def is_match(self, filt_fields):
        for name, value in filt_fields.items():
            v = self.get_field_value(name)
            if v != value:
                return False
        return True

    def get_field_num(self):
        return len(self.fields)

    def get_tuple_id(self):
        return self.__tuple_id

    def get_page_id(self):
        return self.__tuple_id.get_page_id()

    def get_field(self, field_name):
        for f in self.fields:
            n = f.get_name()
            if n == field_name:
                return f
        return None

    def get_field_value(self, field_name):
        for f in self.fields:
            n = f.get_name()
            if n == field_name:
                return f.get_value()
        return None

    def get_field_types(self):
        types = []
        for f in self.fields:
            t = f.get_type()
            types.append(t)
        return types

    def get_fields(self):
        return self.fields


class TupleId:
    def __init__(self, page_id, position):
        self.__page_id = page_id
        self.__position = position

    def equal(self, another):
        if self.get_page_id() == another.get_page_id():
            if self.get_position() == another.get_position():
                return True
        return False

    def get_page_id(self):
        return self.__page_id

    def get_position(self):
        return self.__position
