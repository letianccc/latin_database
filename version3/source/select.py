
from version3.source.filter_ import Filter


class Select:
    def __init__(self, table_name, filt_fields=None, target_field_names=None):
        self.table_name = table_name
        self.filt_fields = filt_fields
        self.target_field_names = target_field_names

    def execute(self):
        match_tuples = self.filt()
        if not self.select_all():
            match_tuples = self.select(match_tuples)
        return match_tuples

    def filt(self):
        f = Filter(self.table_name, self.filt_fields)
        match_tuples = f.execute()
        return match_tuples

    def select_all(self):
        return self.target_field_names == None

    def select(self, complete_tuples):
        tuples = []
        for t in complete_tuples:
            new_t = self.select_tuple(t)
            tuples.append(new_t)
        return tuples

    def select_tuple(self):
        fields = []
        for name in self.target_field_names:
            f = self.get_field(name)
            fields.append(f)
        new_tuple = Tuple(fields)
        return new_tuple
