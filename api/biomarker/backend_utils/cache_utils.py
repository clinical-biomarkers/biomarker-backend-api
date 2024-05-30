# TODO : will probbaly move off of TTLCache eventually for another caching mechanism
# TODO : 
# Cachetools is NOT inherently thread-safe, for now in testing it should be fine.
# However, Gunicorn might introduce concurrency issues because it can use multiple 
# workers processes to handle requests. Each worker has its own memory space, and thus
# its own instance of the cache. Eventually, a shared memory caching solution should
# be built out, which will run as a separate service that can be accessed by all 
# worker processes.
from cachetools import TTLCache

# set up cache
batch_cache: TTLCache = TTLCache(maxsize=200, ttl=3600)

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
