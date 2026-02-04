"""LLM-based path judge for CASTS evaluation."""

from collections.abc import Mapping

from openai import OpenAI

from casts.core.interfaces import Configuration


class PathJudge:
    """LLM judge for scoring CASTS traversal paths.

    Uses a configured LLM to evaluate how well a path answers a goal.
    """

    def __init__(self, config: Configuration) -> None:
        """Initialize PathJudge with configuration.

        Args:
            config: Configuration object containing API settings
        """
        llm_cfg = config.get_llm_config()
        api_key = llm_cfg.get("api_key")
        endpoint = llm_cfg.get("endpoint")
        model = llm_cfg.get("model")

        if not api_key or not endpoint:
            raise RuntimeError("LLM credentials missing for verifier")
        if not model:
            raise RuntimeError("LLM model missing for verifier")

        self.model = model
        self.client = OpenAI(api_key=api_key, base_url=endpoint)

    def judge(self, payload: Mapping[str, object]) -> str:
        """Call the LLM judge and return its raw content.

        The concrete scoring logic (e.g. extracting a numeric score or
        parsing JSON reasoning) is handled by the caller, so this method
        only executes the prompt and returns the model's text output.

        Args:
            payload: Dictionary containing at least:
                - instructions: full prompt to send to the model

        Returns:
            Raw text content from the first chat completion choice.
        """
        prompt = payload.get("instructions")

        if not prompt:
            raise ValueError("No instructions provided to LLM judge")

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a strict CASTS path judge."},
                {"role": "user", "content": str(prompt)},
            ],
            temperature=0.0,
            max_tokens=1024,
        )
        content = (response.choices[0].message.content or "").strip()
        # print(f"[debug] LLM Prompt:\n{prompt}")
        # print(f"[debug] LLM Response:\n{content}")
        return content
