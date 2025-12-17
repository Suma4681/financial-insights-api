import os
from google import genai

MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

def ask_gemini(prompt: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY env var")

    client = genai.Client(api_key=api_key)

    resp = client.models.generate_content(
        model=MODEL,
        contents=prompt,
    )
    return resp.text or ""
