import time

class LockManager:
    def __init__(self):
        self.locklist = dict()
        self.waitlist = dict()
        self.source = dict()

    def acquire_lock(self, tran, page_id, perm):
        if tran == None:
            return
        if self.have_perm(tran, page_id, perm):
            return
        self.acquire(tran, page_id, perm)

    def have_perm(self, tran, page_id, perm):
        locks = self.get_locks(page_id)
        for lock in locks:
            if lock.match_holder(tran):
                if lock.get_type() == perm:
                    return True
        return False

    def acquire(self, tran, page_id, perm):
        self.enter_wait(tran, page_id)
        while True:
            if self.can_acquire(tran, page_id, perm):
                self.grant_lock(tran, page_id, perm)
                self.quit_wait(tran)
                return
            else:
                self.check_deadlock(tran, page_id)
                self.wait()

    def can_acquire(self, tran, page_id, perm):
        locks = self.get_locks(page_id)
        if not self.have_holder(locks):
            return True
        if not self.other_hold(locks, tran):
            return True
        if self.lock_conflict(locks, perm):
            return False
        else:
            return True

    def check_deadlock(self, tran, page_id):
        if self.have_deadlock(tran, page_id):
            self.quit_wait(tran)
            raise DeadlockException

    def have_deadlock(self, tran, page_id):
        locks = self.get_locks(page_id)
        for lock in locks:
            h = lock.get_holder()
            if h != tran:
                if self.is_wait_source(h, tran):
                    return True
        return False

    def is_wait_source(self, holder, applicant):
        if holder in self.waitlist:
            pid = self.waitlist[holder]
            locks = self.get_locks(pid)
            for lock in locks:
                h = lock.get_holder()
                if h == applicant:
                    return True
                return self.is_wait_source(h, applicant)
        return False

    def release_lock(self, tran):
        locks = self.source[tran]
        for lock in locks:
            pid = lock.get_page_id()
            self.release(tran, pid, lock)

    def release(self, tran, page_id, lock):
        locks = self.get_locks(page_id)
        locks.remove(lock)
        if not locks:
            self.locklist.pop(page_id)
        self.remove_source(tran, page_id)

    def grant_lock(self, tran, page_id, perm):
        locks = self.get_locks(page_id)
        if not locks:
            locks = list()
            self.locklist[page_id] = locks
        lock = Lock(tran, page_id, perm)
        locks.append(lock)
        self.add_source(tran, lock)

    def add_source(self, tran, lock):
        if tran not in self.source:
            self.source[tran] = list()
        s = self.source[tran]
        s.append(lock)

    def remove_source(self, tran, page_id):
        locks = self.source[tran]
        for lock in locks:
            if lock.match_page(page_id):
                target = lock
                break
        locks.remove(target)
        if not self.source[tran]:
            self.source.pop(tran)

    def enter_wait(self, tran, page_id):
        self.waitlist[tran] = page_id

    def quit_wait(self, tran):
        self.waitlist.pop(tran)

    def have_holder(self, locks):
        if locks == None:
            return False
        return True

    def other_hold(self, locks, tran):
        for lock in locks:
            if not lock.match_holder(tran):
                return True
        return False

    def wait(self):
        wait_time = 5
        time.sleep(wait_time)

    def have_lock(self, page_id):
        locks = self.get_locks(page_id)
        if not locks:
            return False
        return True

    def lock_conflict(self, locks, perm):
        if perm == 'X':
            return True
        if self.have_X(locks):
            return True
        return False

    def have_X(self, locks):
        for lock in locks:
            t = lock.get_type()
            if t == 'X':
                return True
        return False

    def get_locks(self, page_id):
        for pid, locks in self.locklist.items():
            if pid.equal(page_id):
                return locks
        return []

    def clear(self):
        self.locklist = dict()
        self.waitlist = dict()

class Lock:
    def __init__(self, tran, page_id, lock_type):
        self.type = lock_type
        self.holder = tran
        self.page_id = page_id

    def get_type(self):
        return self.type

    def get_holder(self):
        return self.holder

    def match_holder(self, tran):
        return self.holder == tran

    def match_page(self, other_page_id):
        if self.page_id.equal(other_page_id):
            return True
        else:
            return False

    def set_type(self, type_):
        self.type = type_

    def get_page_id(self):
        return self.page_id

class DeadlockException(Exception):
    pass


# class LockManager:
#     def __init__(self):
#         self.locklist = dict()
#         self.waitlist = dict()
#         self.source = dict()
#
#     def acquire_lock(self, tran, page_id, lock_type):
#         if tran == None:
#             return
#         if self.have_perm(tran, page_id, lock_type):
#             return
#         self.enter_wait(tran, page_id)
#         while True:
#             if self.can_acquire(tran, page_id, lock_type):
#                 self.quit_wait(tran)
#                 self.grant_lock(tran, page_id, lock_type)
#                 if not self.have_deadlock():
#                     return
#             else:
#                 self.wait()
#
#     def have_deadlock(self):
#         pass
#
#     def release_lock(self, tran):
#         for page_id, lock in self.locklist.items():
#             if lock.match_holder(tran):
#                 self.release(tran, page_id)
#
#     def release(self, tran, page_id):
#         lock = self.get_lock(page_id)
#         lock.remove_holder(tran)
#         if not lock.have_holder():
#             self.locklist.pop(page_id)
#         # self.remove_source(tran, page_id)
#
#     def grant_lock(self, tran, page_id, lock_type):
#         lock = self.get_lock(page_id)
#         if lock == None:
#             self.add_lock(tran, page_id, lock_type)
#             return
#         if lock_type == 'X':
#             self.upgrade_lock(lock, lock_type)
#         elif lock_type == 'S':
#             lock.add_holder(tran)
#         # self.add_source(tran, page_id)
#
#     # def get_source(self, tran):
#     #     if tran in self.source:
#     #         s = self.source[tran]
#     #         if s == None:
#     #             self.source[tran] = list()
#     #         return self.source[tran]
#
#     def add_source(self, tran, page_id):
#         lock = self.get_lock(page_id)
#         if tran not in self.source:
#             s = list()
#             self.source[tran] = s
#         s.append(lock)
#
#     def remove_source(self, tran, page_id):
#         locks = self.source[tran]
#         for lock in locks:
#             if lock.match_page(page_id):
#                 target = lock
#                 break
#         locks.remove(target)
#
#
#     def enter_wait(self, tran, page_id):
#         self.waitlist[tran] = page_id
#
#     def quit_wait(self, tran):
#         self.waitlist.pop(tran)
#
#     def upgrade_lock(self, tran, page_id, lock_type):
#         lock = self.get_lock(page_id)
#         lock.set_type(lock_type)
#
#     def add_lock(self, tran, page_id, lock_type):
#         lock = Lock(lock_type, page_id)
#         lock.add_holder(tran)
#         self.locklist[page_id] = lock
#
#     def join_lock(self, tran, page_id):
#         lock = self.get_lock(page_id)
#         lock.add_holder(tran)
#
#     def have_perm(self, tran, page_id, target_perm):
#         if self.hold_lock(tran, page_id):
#             t = self.get_lock_type(page_id)
#             if t == target_perm:
#                 return True
#         return False
#
#     def hold_lock(self, tran, page_id):
#         if not self.have_lock(page_id):
#             return False
#         lock = self.get_lock(page_id)
#         if tran in lock.holders:
#             return True
#         else:
#             return False
#
#     def can_acquire(self, tran, page_id, lock_type):
#         if not self.have_lock(page_id):
#             return True
#         lock = self.get_lock(page_id)
#         # 拥有读锁，申请写锁
#         if self.is_sole_holder(lock, tran):
#             return True
#         if self.lock_conflict(lock, lock_type):
#             return False
#         else:
#             return True
#
#     def is_sole_holder(self, lock, tran):
#         holders = lock.holders
#         if len(holders) == 0:
#             if holders[0] == tran:
#                 return True
#         return False
#
#     def wait(self):
#         wait_time = 5
#         time.sleep(wait_time)
#
#     def have_lock(self, page_id):
#         lock = self.get_lock(page_id)
#         if lock == None:
#             return False
#         if lock.have_holder():
#             return True
#         else:
#             return False
#
#     def lock_conflict(self, lock, lock_type):
#         cur_type = lock.get_type()
#         target_type = lock_type
#         if cur_type == 'S' and target_type == 'S':
#             return False
#         return True
#
#     def get_lock(self, page_id):
#         for pid, lock in self.locklist.items():
#             if pid.equal(page_id):
#                 return lock
#         return None
#
#     def get_lock_type(self, page_id):
#         lock = self.get_lock(page_id)
#         if lock:
#             return lock.get_type()
#
#     def clear(self):
#         self.locklist = dict()
#         self.waitlist = dict()
#
#
# class Lock:
#     def __init__(self, lock_type, page_id):
#         self.type = lock_type
#         self.holders = list()
#         self.page_id = page_id
#
#     def add_holder(self, tran):
#         self.holders.append(tran)
#
#     def remove_holder(self, tran):
#         self.holders.remove(tran)
#
#     def get_type(self):
#         return self.type
#
#     def have_holder(self):
#         return len(self.holders) > 0
#
#     def match_holder(self, tran):
#         if tran in self.holders:
#             return True
#         else:
#             return False
#
#     def match_page(self, other_page_id):
#         if self.page_id.equal(other_page_id):
#             return True
#         else:
#             return False
#
#     def set_type(self, type_):
#         self.type = type_
