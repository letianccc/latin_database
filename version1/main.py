import pickle
import struct
from tree import Tree, TreeRef
import os


def main():
    f = open('test.db', 'r+b')
    f.truncate()
    key = 'a'
    value = 1

    root = get_root(f)
    insert_(root, key, value, f)
    root = get_root(f)
    insert_(root, 'b', 2, f)
    f.seek(0)
    value = get(root, 'b', f)
    print(value)

def get_all_node(f, values):
    ref = get_root(f)
    node = ref_to_node(ref)
    values.append(node.value)
    if node.left_ref:
        pass


def insert_(tree_ref, key, value, f):
    tree_ref = set_(tree_ref, key, value, f)
    commit(tree_ref, f)

def set_(tree_ref, key, value, f):
    node = ref_to_node(tree_ref, f)
    if node == None:
        left = None
        right = None
        address = None
        node = Tree(key, value, left, right, address)
        tree_ref = TreeRef()
    elif key < node.key:
        new_node = set_(node.left_ref, key, value, f)
        node.left_ref = new_node
    elif key > node.key:
        new_node = set_(node.right_ref, key, value, f)
        node.right_ref = new_node
    else:
        node.value = value
    tree_ref.is_change = True
    tree_ref.node = node
    return tree_ref


def commit(tree_ref, f):
    if tree_ref != None and tree_ref.is_change == True:
        commit(tree_ref.node.left_ref, f)
        commit(tree_ref.node.right_ref, f)
        commit_(tree_ref, f)

def commit_(tree_ref, f):
    if tree_ref.address == None:
        tree_ref.address = f.seek(0, os.SEEK_END)
    f.seek(tree_ref.address)
    node = tree_ref.node
    b = tree_to_byte(node)
    len_ = len(b)
    write_integer(f, len_)
    f.write(b)
    f.flush()
    tree_ref.is_change = False


def get_root(f):
    f.seek(0)
    ref = TreeRef(address=0)
    return ref

def ref_to_node(tree_ref, f):
    if os.stat("test.db").st_size == 0:
        return None
    if tree_ref == None:
        return None
    ad = tree_ref.address
    f.seek(ad)

    len_ = read_integer(f)
    byte_ = f.read(len_)
    node = pickle.loads(byte_)

    # try:
    #     len_ = read_integer(f)
    #     byte_ = f.read(len_)
    #     node = pickle.loads(byte_)
    # except:
    #     print('ref_to_node len error')
    #     node = None
    return node


def tree_to_byte(tree):
    byte = pickle.dumps(tree)
    return byte

def select(key, f):
    node = get_root(f)
    while node != None:
        if node.key < key:
            node = get(node.left, f)
        elif node.key > key:
            return get(node.right, key, f)
        elif node.key == key:
            return node.value

def get(root, key, f):
    node = ref_to_node(root, f)
    while node:
        if key < node.key:
            ref = node.left_ref
            if ref == None:
                raise KeyError
            node = ref_to_node(ref, f)
        elif key > node.key:
            ref = node.right_ref
            if ref == None:
                raise KeyError
            node = ref_to_node(ref, f)
        else:
            return node.value







def bytes_to_integer(integer_bytes):
    INTEGER_FORMAT = '!Q'
    i = struct.unpack(INTEGER_FORMAT, integer_bytes)[0]
    return i

def integer_to_bytes(integer):
    INTEGER_FORMAT = '!Q'
    byte = struct.pack(INTEGER_FORMAT, integer)
    return byte

def read_integer(f):
    INTEGER_LENGTH = 8
    byte = f.read(INTEGER_LENGTH)
    i = bytes_to_integer(byte)
    return i

def write_integer(f, integer):
    b = integer_to_bytes(integer)
    f.write(b)





main()
