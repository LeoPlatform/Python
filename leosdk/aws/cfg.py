import os


class Cfg:
    def __init__(self, leo_config):
        self.cfg = self.__by_environment(leo_config)

    def value(self, key):
        val = getattr(self.cfg, key, None)()
        if self.__is_valid(val):
            return key.strip()
        else:
            return None

    def value_or_else(self, key, or_else):
        val = self.value(key)
        if val is not None:
            return val
        else:
            return or_else

    @staticmethod
    def __by_environment(leo_config):
        env = os.getenv('PYTHON_ENV', 'dev')
        cfg = getattr(leo_config, env, None)()
        if cfg is not None:
            return cfg
        else:
            raise AssertionError("'%s' class is missing in leo_config.py" % env)

    @staticmethod
    def __is_valid(key):
        return isinstance(key, str) and key.strip().__len__() > 0
