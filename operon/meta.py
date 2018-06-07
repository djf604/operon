class _MetaSiteDynamic(object):
    def __init__(self, description=None):
        self.description = description


class Meta(object):
    _sites = dict()
    _default_site = None

    @classmethod
    def define_site(cls, name, resources):
        cls._sites[name] = resources

    @classmethod
    def set_default_site(cls, name):
        if name in cls._sites:
            cls._default_site = name

    @staticmethod
    def dynamic(description=None):
        return _MetaSiteDynamic(description)
