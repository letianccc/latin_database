


class FieldDesc:
    def __init__(self, name, type_):
        self.__name = name
        self.__type = type_

    def equal(self, other):
        if self.get_name() == other.get_name():
            if self.get_type().equal(other.get_type()):
                return True
        return False

    def get_size(self):
        return self.__type.get_size()

    def get_type(self):
        return self.__type

    def get_name(self):
        return self.__name

class Field:
    def __init__(self, field_desc, field_value):
        self.descpriter = field_desc
        self.__value = field_value

    def get_type(self):
        return self.descpriter.get_type()

    def get_value(self):
        return self.__value

    def get_name(self):
        return self.descpriter.get_name()

    def equal(self, other):
        if self.descpriter.equal(other.descpriter):
            if self.get_value() == other.get_value():
                return True
        return False
