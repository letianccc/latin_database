class Tuple:
    def __init__(self, fields):
        self.fields = fields

    def equal(self, other):
        n = self.get_field_num()
        for i in range(n):
            f1 = other.fields[i]
            f2 = self.fields[i]
            if not f1.equal(f2):
                return False
        return True

    def match(self, filter_fields):
        for name, value in filter_fields.items():
            value_ = self.get_field_value(name)
            if value != value_:
                return False
        return True

    def get_field_num(self):
        return len(self.fields)

    def get_field(self, field_name):
        for f in self.fields:
            n = f.get_name()
            if n == field_name:
                return f
        return None

    def select(self, target_field_names):
        partial_fields = []
        for name in target_field_names:
            f = self.get_field(name)
            partial_fields.append(f)
        new_tuple = Tuple(partial_fields)
        return new_tuple

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
