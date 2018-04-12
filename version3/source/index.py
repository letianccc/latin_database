class BTree:
    def __init__(self, ):
        self.root = Node()
        self.root.set_parent(self)

    def insert(self, key, value):
        self.root.insert(key, value)

    def delete(self, key):
        self.root.delete(key)

    def insert_node(self, key, node):
        old_root = self.root
        childs = [node, old_root]
        self.root = Node()
        self.root.set_node([key], childs)
        self.root.set_parent(self)
        node.set_parent(self.root)
        old_root.set_parent(self.root)

    def search(self, key):
        return self.root.search(key)

class Node:
    def __init__(self):
        self.parent = None
        self.order = 1
        self.keys = list()
        self.childs = list()

    def insert(self, key, value):
        if not self.keys:
            self.init(key, value)
        else:
            child = self.get_child(key)
            child.insert(key, value)
            if self.full():
                return self.split()

    def delete(self, key):
        child = self.get_child(key)
        child.delete(key)

    def full(self):
        return len(self.keys) > 2 * self.order

    def split(self):
        parent_key = self.keys[self.order]
        n = self.split_node()
        self.parent.insert_node(parent_key, n)

    def split_node(self):
        keys = self.keys[:self.order]
        childs = self.childs[:self.order+1]
        n = Node()
        n.set_node(keys, childs)
        for c in n.childs:
            c.set_parent(n)
        for i in range(self.order+1):
            self.keys.pop(0)
            self.childs.pop(0)
        return n

    def insert_node(self, key, node):
        pos = len(self.keys)
        for i in range(len(self.keys)):
            if key < self.keys[i]:
                pos = i
                break
        self.keys.insert(pos, key)
        self.childs.insert(pos, node)
        node.parent = self
        if node.__class__.__name__ == 'Leave':
            left = self.childs[pos-1]
            right = self.childs[pos+1]
            left.right = node
            right.left = node
            node.left = left
            node.right = right

    def max_key(self):
        return max(self.keys)

    def init(self, key, value):
        self.keys.append(key)
        left = Leave()
        right = Leave()
        left.set_parent(self)
        right.set_parent(self)
        left.right = right
        right.left = left
        left.insert(key, value)
        self.childs.append(left)
        self.childs.append(right)

    def set_node(self, keys, childs):
        self.keys = keys
        self.childs = childs

    def set_parent(self, parent):
        self.parent = parent

    def search(self, key):
        if not self.keys:
            return None
        child = self.get_child(key)
        # if key == 3:
        #     print(child.search(key))
        return child.search(key)

    def get_child(self, key):
        child = self.childs[-1]
        for i in range(len(self.keys)):
            if key <= self.keys[i]:
                child = self.childs[i]
                break
        return child

    def get_index(self, child):
        return self.childs.index(child)


    def set_key(self, index, key):
        self.keys[index] = key

    def delete_child(self, child):
        index = self.get_index(child)
        left = child.left
        right = child.right
        left.right = right
        right.left = left
        self.childs.pop(index)
        if index == len(self.keys):
            index -= 1
        self.keys.pop(index)
        if not self.keys:
            k = self.childs[0].max_key()
            self.keys.append(k)

class Leave:
    def __init__(self):
        self.entry = dict()
        self.order = 1
        self.left = None
        self.right = None
        self.parent = None

    def set_entry(self, entry):
        self.entry = entry

    def set_parent(self, parent):
        self.parent = parent

    def max_key(self):
        return max(self.entry.keys())

    def insert(self, key, value):
        self.entry[key] = value
        if self.full():
            return self.split()

    def full(self):
        return len(self.entry) > 2 * self.order

    def split(self):
        leave = self.split_leave()
        max_key = leave.max_key()
        self.parent.insert_node(max_key, leave)

    def split_leave(self):
        new_entry = dict()
        front_keys = sorted(self.entry.keys())[:self.order]
        for k in front_keys:
            new_entry[k] = self.entry[k]
            self.entry.pop(k)
        leave = Leave()
        leave.set_entry(new_entry)
        return leave

    def search(self, key):
        if key in self.entry:
            v = self.entry[key]
            return v
        return None

    def delete(self, key):
        if key not in self.entry:
            raise Exception
        self.entry.pop(key)
        if self.less_order():
            sibling = self.right
            if self.left:
                if self.left.parent == self.parent:
                    sibling = self.left
            if sibling:
                if sibling.can_allot():
                    self.reallot(sibling)
                else:
                    self.merge(sibling)
            else:
                raise Exception

    def reallot(self, sibling):
        if sibling == self.left:
            k, v = sibling.pop_entry(-1)
            self.entry[k] = v
            sibling.update_key()
        elif sibling == self.right:
            k, v = sibling.pop_entry(0)
            self.entry[k] = v
            self.update_key()
        else:
            raise Exception

    def merge(self, sibling):
        if sibling == self.left:
            garbage = sibling
            leftover = self
        elif sibling == self.right:
            garbage = self
            leftover = sibling
        else:
            raise Exception
        for k, v in garbage.entry.items():
            leftover.insert(k, v)
        garbage.parent.delete_child(garbage)


    def pop_entry(self, index):
        k = sorted(self.entry.keys())[index]
        v = self.entry.pop(k)
        return k, v

    def update_key(self):
        index = self.parent.get_index(self)
        if index < len(self.parent.keys):
            k = self.max_key()
            self.parent.set_key(index, k)

    def less_order(self):
        return len(self.entry) < self.order

    def can_allot(self):
        return len(self.entry) > self.order

    def get_elements(self):
        return list(self.entry.items())



def test_split():
    t = BTree()
    t.insert(1, 'a')
    t.insert(2, 'b')
    assert (t.root.keys == [1])
    assert(t.root.childs[0].get_elements() == [(1, 'a')])
    assert(t.root.childs[1].get_elements() == [(2, 'b')])
    t.insert(3, 'c')
    t.insert(4, 'd')
    assert (t.root.keys == [1, 2])
    assert (list(t.root.childs[0].get_elements()) == [(1, 'a')])
    assert (list(t.root.childs[1].get_elements()) == [(2, 'b')])
    assert (list(t.root.childs[2].get_elements()) == [(3, 'c'), (4, 'd')])
    t.insert(5, 'e')
    assert (t.root.keys == [2])
    assert (t.root.childs[0].keys == [1])
    assert (t.root.childs[1].keys == [3])
    assert (list(t.root.childs[0].childs[0].get_elements()) == [(1, 'a')])
    assert (list(t.root.childs[0].childs[1].get_elements()) == [(2, 'b')])
    assert (list(t.root.childs[1].childs[0].get_elements()) == [(3, 'c')])
    assert (list(t.root.childs[1].childs[1].get_elements()) == [(4, 'd'), (5, 'e')])

def test_search():
    t = BTree()
    start = ord('a')
    n = 26
    for i in range(n):
        t.insert(i+1, chr(start+i))
    for i in range(n):
        v = t.search(i+1)
        try:
            assert(v == chr(start+i))
        except:
            print(i+1, v)


def test_sibling1():
    t = BTree()
    start = ord('a')
    n = 5
    for i in range(n):
        t.insert(i+1, chr(start+i))
    leave = t.root.childs[0].childs[0]
    assert(leave.left == None)
    assert(leave.right.get_elements() == [(2, 'b')])
    leave = leave.right
    assert(leave.right.get_elements() == [(3, 'c')])
    leave = leave.right
    assert(leave.right.get_elements() == [(4, 'd'), (5, 'e')])
    assert(leave.right.right == None)

def test_sibling2():
    t = BTree()
    start = ord('a')
    n = 26
    for i in range(n):
        t.insert(i+1, chr(start+i))

    leave = t.root
    while leave.__class__.__name__ != 'Leave':
        leave = leave.childs[0]
    i = 0
    while leave:
        keys = sorted(leave.entry.keys())
        for k in keys:
            v = leave.entry[k]
            assert(v == chr(start+i))
            i += 1
        leave = leave.right

def test_parent():
    t = BTree()
    start = ord('a')
    n = 26
    for i in range(n):
        t.insert(i+1, chr(start+i))
    leave = t.root
    while leave.__class__.__name__ != 'Leave':
        leave = leave.childs[0]
    i = 0
    while leave != None:
        try:
            assert(leave.parent != None)
        except:
            print(i, leave.entry)
        i += 1
        leave = leave.right

def test_delete():
    t = BTree()
    start = ord('a')
    n = 26
    for i in range(n):
        t.insert(i+1, chr(start+i))
    t.delete(5)
    # print_leave(t)
    # print(t.search(4))
    for i in range(n-1):
        if i == 4:
            continue
        v = t.search(i+1)
        try:
            assert(v == chr(start+i))
        except:
            print(i+1, v)

def print_leave(tree):
    leave = tree.root
    while leave.__class__.__name__ != 'Leave':
        leave = leave.childs[0]
    i = 0
    j = 0
    leaves = dict()
    while leave:
        entrys = []
        for k, v in leave.entry.items():
            entrys.append((k, v))
            i += 1
        leaves[j] = entrys
        j += 1
        leave = leave.right
    for i in range(j):
        print(i, leaves[i])




test_split()
test_search()
test_sibling1()
test_sibling2()
test_parent()
test_delete()
