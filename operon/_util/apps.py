class _DeferredApp(object):
    def __init__(self, app_id):
        self.app_id = app_id

    def __str__(self):
        return self.app_id


class _ParslAppBlueprint(object):
    _id_counter = 0
    _blueprints = dict()
    _app_futures = dict()

    @classmethod
    def get_id(cls):
        cls._id_counter += 1
        return cls._id_counter