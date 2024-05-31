# TODO :
# Cachetools is NOT inherently thread-safe, for now since the default gunicorn setup
# only uses 1 single threaded worker its fine. However, if the number of threads ever
# increases then a manual locking mechanism will have to be introduced. If the number
# of workers ever increases then we'll have to move to a shared memory caching model as
# each worker has its own memory space, and thus its own instance of the cache.
# Eventually, a shared memory caching solution should be built out, which will run as
# a separate service that can be accessed by all worker processes.
from cachetools import LRUCache

# set up cache
batch_cache: LRUCache = LRUCache(maxsize=300)


def generate_cache_key(list_id: str, batch_idx: int) -> str:
    """Generates the object cache key.

    Parameters
    ----------
    list_id : str
        The list_id being queried for.
    batch_idx : int
        The index of the batch.

    Returns
    -------
    str
        The object cache key.
    """
    return f"{list_id}:{batch_idx}"
