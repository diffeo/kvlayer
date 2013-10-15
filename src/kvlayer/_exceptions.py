

class KVLayerError(Exception):
    pass


class ConfigurationError(KVLayerError):
    pass


class ProgrammerError(KVLayerError):
    pass


class StorageClosed(KVLayerError):
    pass


class MissingID(KVLayerError):
    pass


class DatabaseEmpty(KVLayerError):
    pass
