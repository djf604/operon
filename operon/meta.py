class _MetaExecutorDynamic(object):
    def __init__(self, description=None):
        self.description = description


class Meta(object):
    _executors = dict()
    _default_executor = None

    @classmethod
    def define_executor(cls, label, resources):
        cls._executors[label] = resources

    @classmethod
    def set_default_executor(cls, label):
        if label in cls._executors:
            cls._default_executor = label

    @classmethod
    def define_site(cls, name, resources):
        """
        This is kept around for back compatibility
        """
        cls.define_executor(label=name, resources=resources)

    @classmethod
    def set_default_site(cls, name):
        """
        This is kept around for back compatibility
        """
        cls.set_default_executor(label=name)

    @staticmethod
    def dynamic(description=None):
        return _MetaExecutorDynamic(description)
