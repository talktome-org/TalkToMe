from pathlib import Path
from typing import Optional


def _backend_root() -> Path:
    # Backend/
    return Path(__file__).resolve().parents[1]


class VoiceAgentPromptLibrary:
    """
    Loads per-voice-agent system prompts for voice-first (ephemeral) chat.

    Prompts live under:
      Backend/resources/elevenlabs_prompts/<agent>_prompt.txt

    Agent keys are case-insensitive and whitespace-trimmed (e.g. "volt", "Volt ").
    """

    def __init__(self):
        self._prompts: dict[str, str] = {}

        prompts_dir = _backend_root() / "resources" / "elevenlabs_prompts"
        for name in ("volt", "luma", "pax", "mira", "hexley"):
            path = prompts_dir / f"{name}_prompt.txt"
            try:
                text = path.read_text(encoding="utf-8").strip()
                if text:
                    self._prompts[name.lower()] = text
            except FileNotFoundError:
                # Prompts are expected to exist in prod; keep server boot resilient in dev.
                continue

    @property
    def available_agents(self) -> list[str]:
        return sorted({k.capitalize() for k in self._prompts.keys()})

    def get_prompt(self, voice_agent: Optional[str]) -> Optional[str]:
        """
        Returns the prompt for the given voice agent, or None if not found.
        The client is expected to always send a valid agent name.
        """
        key = (voice_agent or "").strip().lower()
        return self._prompts.get(key) if key else None
