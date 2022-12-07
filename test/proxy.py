import pickle
from multiprocessing.managers import SyncManager, BaseProxy


class Complex:
    def __init__(self):
        f = open('/tmp/d', 'w')

    def write(self, b):
        f.write(b)


class ComplexProxy(BaseProxy):

    def __init__(self,  token, serializer, manager=None,
                 authkey=None, exposed=None, incref=True, manager_owned=False):
        print(f">>>>>>>>>> {token} {serializer} {manager} {authkey} {exposed} {manager_owned}")
        super().__init__(token, serializer, manager, authkey, exposed, incref, manager_owned)


SyncManager.register("Complex", callable=Complex, proxytype=ComplexProxy)

mp = SyncManager(address=('localhost', 0))
mp.start()
c = mp.Complex()
c.write('b')

pickle