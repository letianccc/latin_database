from version2.source.heappage import HeapPage

class HeapFile:
    def __init__(self, table_id, file_manager, page_num):
        self.__table_id = table_id
        self.__page_num = 0
        self.__pages = list()
        self.__file_manager = file_manager



    def get_usable_page(self):
        for p in self.__pages:
            if p.has_empty_entry():
                return p
        p = self.create_new_page()
        return p


        for i in range(self.__page_num):
            pass

    def get_all_tuples(self):
        n = self.get_page_num()
        all_tuples = []
        for page_id in range(n):
            tuples = self.read_page(page_id)
            all_tuples += tuples
        return all_tuples

    def create_new_page(self):
        page_id = self.__page_num
        p = HeapPage(self.__table_id, page_id)
        self.__pages.append(p)
        self.__page_num += 1
        self.flush_page_num()
        return p

    def flush_page_num(self):
        n = self.__page_num
        self.__file_manager.flush_page_num(n)

    def flush_page(self, page):
        self.__file_manager.flush_page(page)
        page.mark_clear()

    def id2page(self, page_id):
        p = self.__pages[page_id]
        return p

    def read_page(self, page_id):
        return self.__file_manager.read_page(page_id)

    def get_page_num(self):
        return self.__page_num
