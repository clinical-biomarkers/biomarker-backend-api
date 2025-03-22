import openai
from openai.types.chat import ChatCompletionMessageParam
from flask import current_app
from time import sleep
from typing import Dict, List, Optional

from . import LLM
from ..db import cast_app


class OpenAILLM(LLM):

    def __init__(self, api_key_name: str = "LLM_API_KEY", max_tokens: int = 1000, max_retries: int = 2):
        super().__init__(api_key_name, max_tokens, max_retries)
        self.instance = openai.OpenAI()
        super().__init__(api_key_name)

    def advanced_search(self, query: str) -> Optional[Dict]:
        custom_app = cast_app(current_app)

        if not self._validate_api_key():
            custom_app.api_logger.warning("Could not verify OpenAI API key")
            return None

        messages: List[ChatCompletionMessageParam] = [
            {"role": "system", "content": self._full_search_system_prompt},
            {"role": "user", "content": query},
        ]
        validated_response = None
        for i in range(self._max_retries):
            try:
                response = self.instance.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=messages,
                    max_tokens=self._max_tokens,
                )

                response_text = response.choices[0].message.content
                if response_text is None:
                    sleep((i + 1) ** 2)
                    continue

                validated, validated_response, error_message = (
                    self.validate_advanced_search_response(llm_response=response_text)
                )
                if validated:
                    break

                feedback_message: ChatCompletionMessageParam = {
                    "role": "user",
                    "content": f"Your response could not be validated. Please correct the folowing issues and provide valid JSON: {error_message}",
                }
                messages.append(feedback_message)
                sleep(0.5)

            except Exception as e:
                custom_app.api_logger.error(f"Error trying to use OpenAI API: {e}")
                sleep((i + 1) ** 2)

        return validated_response
