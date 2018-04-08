from version2.source.catalog import Catalog

class BufferPool:
    def __init__(self):
        self.buffer_size = 4096
        self.entrys = list()
        self.heapfiles = dict()

    def insert_tuple(self, table_name, tuple_):
        if self.is_valid_table(table_name):
            p = self.get_usable_page(table_name)
            p.insert_tuple(tuple_)
            self.flush_buffer()


    def is_valid_table(self, table_name):
        result = Catalog.is_valid_table(table_name)
        return result

    def get_all_tuples(self, table_name):
        hf = self.name2hf(table_name)
        tuples = hf.get_all_tuples()
        return tuples

    def select_tuples(self, table_name, target_field_names, filter_fields):
        tuples = self.get_all_tuples(table_name)
        tuples = self.get_filter_tuples(tuples, filter_fields)
        tuples = self.select(tuples, target_field_names)
        return tuples

    def select(self, tuples, target_field_names):
        new_tuples = []
        for t in tuples:
            new_tuple = t.select(target_field_names)
            new_tuples.append(new_tuple)
        return new_tuples

    def get_filter_tuples(self, original_tuples, filter_fields):
        filter_tuples = self.filter_(original_tuples, filter_fields)
        return filter_tuples

    def filter_(self, tuples, filter_fields):
        filter_tuples = []
        for t in tuples:
            if t.match(filter_fields):
                filter_tuples.append(t)
        return filter_tuples

    def flush_buffer(self):
        for page in self.entrys:
            if page.is_dirty():
                hf = self.page2hf(page)
                hf.flush_page(page)

    def page2hf(self, page):
        hf = page.get_heapfile()
        return hf

    def name2hf(self, table_name):
        return Catalog.name_to_file(table_name)

    def get_usable_page(self, table_name):
        # hf = Catalog.name_to_file
        # 未命中
        hf = self.name2hf(table_name)
        p = hf.get_usable_page()
        self.entrys.append(p)
        return p
