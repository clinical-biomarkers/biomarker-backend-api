from abc import ABC, abstractmethod
import os
from dotenv import load_dotenv
import json
from typing import Dict, Optional, Tuple
from marshmallow import EXCLUDE
from marshmallow.exceptions import ValidationError

from .search_system_prompt import SEARCH_SYSTEM_PROMPT
from ..data_models import SearchFullSchema


class LLM(ABC):

    def __init__(self, api_key_name: str = "LLM_API_KEY", max_tokens: int = 1_000, max_retries: int = 2):
        load_dotenv()
        self.api_key_name = api_key_name
        self._api_key = os.getenv(self.api_key_name)
        self._max_tokens = max_tokens
        self._max_retries = max_retries
        self._full_search_system_prompt = SEARCH_SYSTEM_PROMPT

    def _validate_api_key(self) -> bool:
        if self._api_key is None:
            return False
        return True

    @abstractmethod
    def advanced_search(self, query: str) -> Optional[Dict]:
        pass

    def validate_advanced_search_response(
        self, llm_response: str
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        try:
            response = json.loads(llm_response)
            validated_response = SearchFullSchema().load(response, unknown=EXCLUDE)
            if not isinstance(validated_response, dict):
                return False, None, "Response is not valid Python dictionary"
            return True, validated_response, None
        except ValidationError as e:
            marshmallow_error = e.messages_dict
            return False, None, str(marshmallow_error)
        except Exception as e:
            return False, None, str(e)
