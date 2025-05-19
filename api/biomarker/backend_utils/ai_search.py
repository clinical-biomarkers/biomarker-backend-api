from flask import Request, current_app
from typing import Tuple, Dict
from pprint import pformat

from . import LLM_PROVIDER
from . import db as db_utils
from . import utils as utils
from .search_utils import _search_query_builder
from .llm import LLM
from .llm.openai_api import OpenAILLM
from .llm.rate_limiter import ai_search_rate_limiter


def ai_full_search(api_request: Request) -> Tuple[Dict, int]:
    """Entry point for the AI-assisted search endpoint."""
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "ai_search"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    user_query = request_arguments["query"]

    # Check rate limit before proceeding
    if not ai_search_rate_limiter.can_make_request():
        rate_status = ai_search_rate_limiter.get_status()
        error_obj = db_utils.log_error(
            error_log="Rate limit exceeded for AI search",
            error_msg="rate-limit-exceeded",
            origin="ai_full_search",
            rate_limit_status=rate_status,
        )
        return error_obj, 429

    # Generate structured search parameters using OpenAI
    search_params, search_params_http_code = _parse_full_search_query_ai(user_query)
    if search_params_http_code != 200:
        return search_params, search_params_http_code

    custom_app = db_utils.cast_app(current_app)
    custom_app.api_logger.info(
        f"AI Full Search\nOriginal Query: `{user_query}`\nParsedParameters: {pformat(search_params)}"
    )

    ai_search_metadata = {
        "original_query": user_query,
        "parsed_parameters": search_params,
    }

    # Perform the search using the parsed parameters
    mongo_query = _search_query_builder(search_params, False)
    return_object, query_http_code = db_utils.search_and_cache(
        request_object=search_params,
        query_object=mongo_query,
        search_type="full",
        ai_search_metadata=ai_search_metadata,
    )

    # Add the original query and parsed parameters to the response
    if query_http_code == 200:
        return_object["ai_parsing"] = ai_search_metadata

    return return_object, query_http_code


def _parse_full_search_query_ai(query: str) -> Tuple[Dict, int]:
    """Parse a natural language query into structured search parameters using OpenAI."""
    custom_app = db_utils.cast_app(current_app)

    try:
        llm_client: LLM
        if LLM_PROVIDER == "openai":
            llm_client = OpenAILLM()
        else:
            custom_app.api_logger.warning(
                f"Unsupported LLM provider: `{LLM_PROVIDER}`, falling back to OpenAI"
            )
            llm_client = OpenAILLM()

        search_params = llm_client.advanced_search(query)

        if search_params is None:
            error_obj = db_utils.log_error(
                error_log=f"Unable to parse query using LLM provider: `{LLM_PROVIDER}`\nquery: {query}",
                error_msg="internal-server-error",
                origin="_parse_full_search_query_ai",
            )
            return error_obj, 500

        if "error" in search_params:
            if search_params["error"] == llm_client.key_error_str:
                error_obj = db_utils.log_error(
                    error_log="Unable to find LLM API key",
                    error_msg="internal-server-error",
                    origin="_parse_full_search_query_ai",
                )
                return error_obj, 500
            elif search_params["error"] == llm_client.relevancy_error_str:
                error_obj = db_utils.log_error(
                    error_log=f"User made non-biomarker query:\n{query}",
                    error_msg="non-biomarker-related-query",
                    origin="_parse_full_search_query_ai",
                )
                return error_obj, 400

        return search_params, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Unable to parse query using LLM provider: `{LLM_PROVIDER}`\nquery: {query}\nerror: {e}",
            error_msg="internal-server-error",
            origin="_parse_full_search_query_ai",
        )
        return error_obj, 500
