class Meta(object):
    _sites = dict()

    @classmethod
    def define_site(cls, name, resources):
        cls._sites[name] = resources