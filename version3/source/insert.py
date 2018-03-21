from version3.source.catalog import Catalog



class Insert:
    def __init__(self, table_name, tuple_, tran_id=None):
        self.table_name = table_name
        self.tuple = tuple_
        self.tran_id = tran_id

    def execute(self):
        hf = Catalog.name_to_file(self.table_name)
        hf.insert_tuple(self.tuple, self.tran_id, 'X')
        
