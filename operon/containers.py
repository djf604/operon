from collections.abc import MutableSequence, MutableMapping
from concurrent.futures import Future
from parsl.dataflow.futures import AppFuture
from parsl.app.futures import DataFuture



class DeferredDict(Future, MutableMapping):
    """
    Mapping type which imitates a dictionary, but resolves all Futures held within when
    it's used as an input to a parsl App
    """
    _mappings = list()

    def __init__(self, dict=None):
        super().__init__()
        self._app_future = None
        self._tid = None
        self._dfk = None

        # DefferedDict is mutable until it's used as parsl input
        self._mutable = True


    def _set_callback_info(self, app_future, tid, dfk):
        self._app_future = app_future
        self._tid = tid
        self._dfk = dfk


    def parent_callback(self, parent_future):
        if parent_future.done():
            if parent_future._exception:
                super().set_exception(parent_future.exception)
            else:
                super().set_result(parent_future.result())

    def finish(self):
        if all((self._dfk, self._tid, self._app_future)):
            with self._condition:
                self._state = 'FINISHED'
                self._dfk.handle_update(self._tid, self._app_future)









class OperonList(MutableSequence):
    _lists = list()

    def __init__(self, iterable=None):
        self._list = list(iterable) if iterable else list()
        OperonList._lists.append(self)

    def __len__(self):
        return len(self._list)

    def __getitem__(self, item):
        return self._list.__getitem__(item)

    def __setitem__(self, key, value):
        return self._list.__setitem__(key, value)

    def __delitem__(self, key):
        return self._list.__delitem__(key)

    def insert(self, index, value):
        return self._list.insert(index, value)

    def __add__(self, other):
        return self._list + other

    def __mul__(self, other):
        return self._list * other

    def __repr__(self):
        return repr(self._list)

    def __unicode__(self):
        return str(self._list)
