
from version3.source.catalog import Catalog


class Filter:
    def __init__(self, table_name, filt_fields):
        self.table_name = table_name
        self.filt_fields = filt_fields

    def execute(self):
        hf = Catalog.name_to_file(self.table_name)
        tuples = hf.get_all_tuples()
        if self.has_filter():
            match_tuples = []
            for t in tuples:
                if t.is_match(self.filt_fields):
                    match_tuples.append(t)
            tuples = match_tuples
        return tuples

    def has_filter(self):
        return self.filt_fields != None
