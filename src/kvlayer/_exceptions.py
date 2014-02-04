

class KVLayerError(Exception):
    pass


class ConfigurationError(KVLayerError):
    pass


class ProgrammerError(KVLayerError):
    pass


class StorageClosed(KVLayerError):
    pass


class MissingID(KVLayerError):
    """A kvlayer function was passed a nonexistent key.

    If the function takes multiple keys or a key range, nothing matched
    at all.

    """
    pass


class DatabaseEmpty(KVLayerError):
    pass


class BadKey(KVLayerError):
    """A key value passed to a kvlayer function was not of the correct form.

    Keys must be tuples of a fixed length of :class:`uuid.UUID` objects.
    The length of the tuple is specified in the initial call to
    :meth:`kvlayer._abstract_storage.AbstractStorage.setup_namespace`.

    """
    pass
