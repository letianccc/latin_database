from version3.source.buffer_pool import BufferPool



class Database:
    __buffer = BufferPool()

    @classmethod
    def get_buffer(cls):
        return cls.__buffer

    @classmethod
    def clear(cls):
        cls.__buffer = BufferPool()
