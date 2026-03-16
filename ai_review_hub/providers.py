import asyncio
import os
from dataclasses import dataclass
from typing import Dict


@dataclass
class ProviderConfig:
    name: str
    model: str


class Providers:
    def __init__(self) -> None:
        self.openai_key = os.getenv("OPENAI_API_KEY", "")
        self.anthropic_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.google_key = os.getenv("GOOGLE_API_KEY", "")

        self.cfgs = [
            ProviderConfig("openai", os.getenv("OPENAI_MODEL", "gpt-4o-mini")),
            ProviderConfig("anthropic", os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")),
            ProviderConfig("gemini", os.getenv("GEMINI_MODEL", "gemini-2.0-flash")),
        ]

    async def ask_all(self, prompt: str) -> Dict[str, str]:
        tasks = [self.ask(c.name, c.model, prompt) for c in self.cfgs]
        outs = await asyncio.gather(*tasks, return_exceptions=True)
        result: Dict[str, str] = {}
        for c, out in zip(self.cfgs, outs):
            if isinstance(out, Exception):
                result[c.name] = f"[ERROR] {type(out).__name__}: {out}"
            else:
                result[c.name] = out
        return result

    async def ask(self, provider: str, model: str, prompt: str) -> str:
        if provider == "openai":
            return await asyncio.to_thread(self._ask_openai, model, prompt)
        if provider == "anthropic":
            return await asyncio.to_thread(self._ask_anthropic, model, prompt)
        if provider == "gemini":
            return await asyncio.to_thread(self._ask_gemini, model, prompt)
        raise ValueError(f"unknown provider: {provider}")

    def _ask_openai(self, model: str, prompt: str) -> str:
        if not self.openai_key:
            return "[SKIP] OPENAI_API_KEY 未配置"
        from openai import OpenAI

        c = OpenAI(api_key=self.openai_key)
        r = c.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        return r.choices[0].message.content or ""

    def _ask_anthropic(self, model: str, prompt: str) -> str:
        if not self.anthropic_key:
            return "[SKIP] ANTHROPIC_API_KEY 未配置"
        import anthropic

        c = anthropic.Anthropic(api_key=self.anthropic_key)
        r = c.messages.create(
            model=model,
            max_tokens=1500,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        parts = []
        for b in r.content:
            if getattr(b, "type", "") == "text":
                parts.append(b.text)
        return "\n".join(parts).strip()

    def _ask_gemini(self, model: str, prompt: str) -> str:
        if not self.google_key:
            return "[SKIP] GOOGLE_API_KEY 未配置"
        from google import genai

        c = genai.Client(api_key=self.google_key)
        r = c.models.generate_content(model=model, contents=prompt)
        return (r.text or "").strip()
