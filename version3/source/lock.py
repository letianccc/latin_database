import time




class LockManager:
    def __init__(self):
        self.locklist = dict()
        self.waitlist = dict()

    def acquire_lock(self, tran_id, page_id, lock_type):
        if tran_id == None:
            return
        if self.have_perm(tran_id, page_id, lock_type):
            return
        self.enter_wait(tran_id, page_id)
        while True:
            if self.can_acquire(tran_id, page_id, lock_type):
                self.quit_wait(tran_id)
                self.grant_lock(tran_id, page_id, lock_type)
                return
            else:
                self.wait()

    def release_lock(self, tran_id):
        for page_id, lock in self.locklist.items():
            if lock.match_holder(tran_id):
                self.release(tran_id, page_id)

    def release(self, tran_id, page_id):
        lock = self.get_lock(page_id)
        lock.remove_holder(tran_id)
        if not lock.have_holder():
            self.locklist.pop(page_id)

    def grant_lock(self, tran_id, page_id, lock_type):
        lock = self.get_lock(page_id)
        if lock == None:
            self.add_lock(tran_id, page_id, lock_type)
            return
        if lock_type == 'X':
            self.upgrade_lock(lock, lock_type)
        elif lock_type == 'S':
            lock.add_holder(tran_id)

    def enter_wait(self, tran_id, page_id):
        self.waitlist[tran_id] = page_id

    def quit_wait(self, tran_id):
        self.waitlist.pop(tran_id)

    def upgrade_lock(self, lock, lock_type):
        lock.set_type(lock_type)

    def add_lock(self, tran_id, page_id, lock_type):
        lock = Lock(lock_type)
        lock.add_holder(tran_id)
        self.locklist[page_id] = lock

    def join_lock(self, tran_id, page_id):
        lock = self.get_lock(page_id)
        lock.add_holder(tran_id)

    def have_perm(self, tran_id, page_id, lock_type):
        if self.hold_lock(tran_id, page_id):
            t = self.get_lock_type(page_id)
            if t == 'X':
                return True
            if t == 'S' and lock_type == 'S':
                return True
        return False

    def hold_lock(self, tran_id, page_id):
        if not self.have_lock(page_id):
            return False
        lock = self.get_lock(page_id)
        if tran_id in lock.holders:
            return True
        else:
            return False

    def can_acquire(self, tran_id, page_id, lock_type):
        if not self.have_lock(page_id):
            return True
        lock = self.get_lock(page_id)
        # 拥有读锁，申请写锁
        if self.is_sole_holder(lock, tran_id):
            return True
        if self.lock_conflict(lock, lock_type):
            return False
        else:
            return True

    def is_sole_holder(self, lock, tran_id):
        holders = lock.holders
        if len(holders) == 0:
            if holders[0] == tran_id:
                return True
        return False

    def wait(self):
        wait_time = 5
        time.sleep(wait_time)

    def have_lock(self, page_id):
        lock = self.get_lock(page_id)
        if lock == None:
            return False
        if lock.have_holder():
            return True
        else:
            return False

    def lock_conflict(self, lock, lock_type):
        cur_type = lock.get_type()
        target_type = lock_type
        if cur_type == 'S' and target_type == 'S':
            return False
        return True

    def get_lock(self, page_id):
        for pid, lock in self.locklist.items():
            if pid.equal(page_id):
                return lock
        return None

    def get_lock_type(self, page_id):
        lock = self.get_lock(page_id)
        if lock:
            return lock.get_type()

    def clear(self):
        self.locklist = dict()
        self.waitlist = dict()


class Lock:
    def __init__(self, lock_type):
        self.type = lock_type
        self.holders = list()

    def add_holder(self, tran_id):
        self.holders.append(tran_id)

    def remove_holder(self, tran_id):
        self.holders.remove(tran_id)

    def get_type(self):
        return self.type

    def have_holder(self):
        return len(self.holders) > 0

    def match_holder(self, tran_id):
        if tran_id in self.holders:
            return True
        else:
            return False

    def set_type(self, type_):
        self.type = type_
