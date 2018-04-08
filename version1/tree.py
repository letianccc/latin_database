


class Tree(object):
    def __init__(self, key=None, value=None, left=None, right=None, address=None):
        self.key = key
        self.value = value
        self.left_ref = left
        self.right_ref = right
        self.address = address


class TreeRef(object):
    def __init__(self, node=None, address=None):
        self.node = node
        self.address = address
        self.is_change = False
