class T:
    def __init__(self):
        self.value = 0

t1 = T()
t2 = T()
t3 = T()
d = dict()
d[t1] = t2
t2.value = 3
t2_ = d[t1]
print(t2_.value)