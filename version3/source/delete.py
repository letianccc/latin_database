from version3.source.filter_ import Filter
from version3.source.catalog import Catalog



class Delete:
    def __init__(self, table_name, filt_fields):
        self.table_name = table_name
        self.filt_fields = filt_fields

    def execute(self):
        # match_tuples = self.filt()
        hf = Catalog.name_to_file(self.table_name)
        # tuples = hf.get_all_tuples()
        # for t in match_tuples:
        hf.delete_tuple(self.filt_fields)

    def filt(self):
        f = Filter(self.table_name, self.filt_fields)
        match_tuples = f.execute()
        return match_tuples
