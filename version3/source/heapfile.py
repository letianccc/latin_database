from version3.source.database import Database
from version3.source.heappage import PageId

class HeapFile:
    def __init__(self, table_id, page_num):
        self.__id = table_id
        self.__page_num = page_num

    def insert_tuple(self, tuple_, tran_id=None, lock_type=None):
        p = self.get_insertable_page(tran_id, lock_type)
        p.insert_tuple(tuple_)

    def delete_tuple(self, filt_fields):
        tid = self.get_id()
        b = Database.get_buffer()
        n = self.get_page_num()
        for pid in range(n):
            pid = PageId(tid, pid)
            p = b.get_page(pid)
            tuples = p.get_match_tuples(filt_fields)
            for t in tuples:
                p.delete_tuple(t)

    def get_insertable_page(self, tran_id=None, lock_type=None):
        tid = self.get_id()
        n = self.get_page_num()
        b = Database.get_buffer()
        for page_index in range(n):
            page_id = PageId(tid, page_index)
            p = b.get_page(page_id, tran_id, lock_type)
            if p.has_empty_entry():
                return p

        self.create_new_page()
        p = self.get_last_page(tran_id, lock_type)
        return p

    def get_last_page(self, tran_id, lock_type):
        n = self.get_page_num()
        pid = n - 1
        tid = self.get_id()
        pid = PageId(tid, pid)
        b = Database.get_buffer()
        p = b.get_page(pid, tran_id, lock_type)

        return p

    def get_all_tuples(self):
        all_tuples = []
        b = Database.get_buffer()
        tid = self.get_id()
        n = self.get_page_num()
        for pid in range(n):
            pid = PageId(tid, pid)
            p = b.get_page(pid)
            all_tuples += p.get_tuples()
        return all_tuples

    def create_new_page(self):
        self.__page_num += 1
        self.flush_page_num()

    def flush_page_num(self):
        n = self.__page_num
        tid = self.get_id()
        b = Database.get_buffer()
        b.flush_page_num(tid, n)

    def get_id(self):
        return self.__id

    def get_page_num(self):
        return self.__page_num
