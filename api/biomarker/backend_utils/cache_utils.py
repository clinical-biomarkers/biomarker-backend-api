# TODO :
# Cachetools is NOT inherently thread-safe, for now since the default gunicorn setup
# only uses 1 single threaded worker its fine. However, if the number of threads ever
# increases then a manual locking mechanism will have to be introduced. If the number
# of workers ever increases then we'll have to move to a shared memory caching model as
# each worker has its own memory space, and thus its own instance of the cache.
# Eventually, a shared memory caching solution should be built out (e.g. Redis), which
# will run as a separate service that can be accessed by all worker processes.
from cachetools import TTLCache
from typing import Any, Dict, Optional
import json

# Cache pipeline results with max 300 entries and a max time to live of 604,800 
# seconds, or two weeks. We are only caching simple search "Biomarker" and 
# "Condition" category results.
PIPELINE_CACHE = TTLCache(maxsize=300, ttl=604_800)


def _should_cache_search(cache_info: Dict) -> bool:
    """Determines if the search results should be cached based on search type. We
    are only caching simple search `Biomarker` and `Condition` category results.
    """
    if cache_info.get("search_type") != "simple":
        return False

    api_request = cache_info.get("api_request", {})
    term_category = api_request.get("term_category", "").lower().strip()

    return term_category in {"biomarker", "condition"}


def generate_pipeline_cache_key(list_id: str, request_args: Dict[str, Any]) -> str:
    filters = json.dumps(request_args.get("filters", []), sort_keys=True)
    sort = request_args.get("sort", "hit_score")
    order = request_args.get("order", "desc")
    offset = request_args.get("offset", 1)
    limit = request_args.get("limit", 20)
    return f"pipeline:{list_id}:{filters}:{sort}:{order}:{offset}:{limit}"


def get_cached_pipeline_results(
    list_id: str, request_args: Dict[str, Any], cache_info: Dict
) -> Optional[Dict[str, Any]]:
    if not _should_cache_search(cache_info=cache_info):
        return None
    cache_key = generate_pipeline_cache_key(list_id=list_id, request_args=request_args)
    return PIPELINE_CACHE.get(cache_key)


def cache_pipeline_results(
    list_id: str,
    request_args: Dict[str, Any],
    results: Dict[str, Any],
    cache_info: Dict,
) -> None:
    if not _should_cache_search(cache_info=cache_info):
        return
    cache_key = generate_pipeline_cache_key(list_id=list_id, request_args=request_args)
    PIPELINE_CACHE[cache_key] = results
