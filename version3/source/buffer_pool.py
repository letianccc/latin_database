from version3.source.file_manager import FileManager
from version3.source.heappage import HeapPage
from version3.source.lock import LockManager



class BufferPool:
    def __init__(self):
        self.buffer_size = 1
        self.entrys = list()
        self.lock_manager = LockManager()

    def get_all_tuples(self, table_name):
        hf = self.name2hf(table_name)
        tuples = hf.get_all_tuples()
        return tuples

    def flush_buffer(self):
        for page in self.entrys:
            if page.is_dirty():
                hf = self.page2hf(page)
                hf.flush_page(page)

    def flush_page_num(self, table_id, page_num):
        FileManager.flush_page_num(table_id, page_num)

    def acquire_lock(self, page_id, tran_id=None, perm=None):
        self.lock_manager.acquire_lock(tran_id, page_id, perm)

    def release_lock(self, tran_id):
        self.lock_manager.release_lock(tran_id)

    def get_page(self, page_id, tran_id=None, perm=None):
        self.acquire_lock(page_id, tran_id, perm)

        for page in self.entrys:
            if page.equal_id(page_id):
                return page
        # 未命中
        tuples = FileManager.read_page(page_id)
        p = HeapPage(page_id, tuples)
        self.cache_page(p)
        return p

    def flush_file(self, table_id):
        for page in self.entrys:
            tid = page.get_table_id()
            if tid == table_id and page.is_dirty():
                FileManager.flush_page(page)
                page.mark_clear()

    def cache_page(self, page):
        if self.buffer_full():
            self.remove_page()
        self.entrys.append(page)

    def buffer_full(self):
        return len(self.entrys) == self.buffer_size

    def remove_page(self):
        p = self.entrys.pop()
        if p.is_dirty():
            self.flush_page(p)

    def flush_page(self, page):
        FileManager.flush_page(page)

    def clear_buffer(self):
        for i in range(len(self.entrys)):
            self.remove_page()

    def get_lock_manager(self):
        return self.lock_manager
