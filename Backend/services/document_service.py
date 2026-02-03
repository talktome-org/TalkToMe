from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import tiktoken
from pypdf import PdfReader


class DocumentProcessor:
    """
    Handles PDF/TXT ingestion:
    - Extract text
    - Tokenize and chunk with overlap
    """

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        *,
        max_pages: Optional[int] = None,
        max_chars: Optional[int] = None,
        use_tokenizer: bool = True,
        chunker: Optional[str] = None,
    ):
        # Defaults match OpenAI tokenizer for most text models.
        self.encoding = tiktoken.get_encoding("cl100k_base")

        self.chunk_size = int(chunk_size)
        self.chunk_overlap = int(chunk_overlap)
        self.max_pages = max_pages
        self.max_chars = max_chars
        # Allow explicit chunker override while keeping backwards compatibility.
        if chunker:
            self.chunker = str(chunker).lower().strip()
        else:
            self.chunker = "tokens" if use_tokenizer else "chars"

    def count_tokens(self, text: str) -> int:
        return len(self.encoding.encode(text))

    def chunk_text(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Chunk either by tokens (default) or by characters (use_tokenizer=False).
        """
        if not (text or "").strip():
            return []

        if self.chunker == "tokens":
            return self._chunk_by_tokens(text=text, metadata=metadata)
        if self.chunker == "chars":
            return self._chunk_by_chars(text=text, metadata=metadata)
        if self.chunker in ("recursive", "langchain"):
            return self._chunk_by_recursive(text=text, metadata=metadata)
        # Fallback to token chunking if unknown mode.
        return self._chunk_by_tokens(text=text, metadata=metadata)

    def _chunk_by_tokens(self, *, text: str, metadata: Dict = None) -> List[Dict]:
        chunks: List[Dict] = []
        tokens = self.encoding.encode(text)
        if not tokens:
            return chunks

        start = 0
        idx = 0
        while start < len(tokens):
            end = min(start + self.chunk_size, len(tokens))
            chunk_tokens = tokens[start:end]
            chunk_text = self.encoding.decode(chunk_tokens)

            chunk_metadata = (metadata or {}).copy()
            chunk_metadata["chunk_index"] = idx
            chunk_metadata["token_count"] = len(chunk_tokens)

            chunks.append({"text": chunk_text, "metadata": chunk_metadata})

            start = end - self.chunk_overlap
            idx += 1
            if start >= end:
                break

        return chunks

    def _chunk_by_chars(self, *, text: str, metadata: Dict = None) -> List[Dict]:
        chunks: List[Dict] = []
        start = 0
        idx = 0
        n = len(text)
        while start < n:
            end = min(start + self.chunk_size, n)
            chunk_text = text[start:end]

            chunk_metadata = (metadata or {}).copy()
            chunk_metadata["chunk_index"] = idx
            chunk_metadata["token_count"] = None

            chunks.append({"text": chunk_text, "metadata": chunk_metadata})

            start = end - self.chunk_overlap
            idx += 1
            if start >= end:
                break
        return chunks

    def _chunk_by_recursive(self, *, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Use LangChain's RecursiveCharacterTextSplitter to create semantically cleaner chunks.
        This avoids tokenizer overhead and handles long lines better than naive splitting.
        """
        try:
            from langchain_text_splitters import RecursiveCharacterTextSplitter
        except Exception:
            # If langchain_text_splitters isn't available, fall back to chars.
            return self._chunk_by_chars(text=text, metadata=metadata)

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            separators=["\n\n", "\n", ". ", " ", ""],
        )
        parts = splitter.split_text(text)
        chunks: List[Dict] = []
        for idx, chunk_text in enumerate(parts):
            chunk_metadata = (metadata or {}).copy()
            chunk_metadata["chunk_index"] = idx
            chunk_metadata["token_count"] = None
            chunks.append({"text": chunk_text, "metadata": chunk_metadata})
        return chunks

    def pdf_to_text(self, pdf_path: Path) -> str:
        reader = PdfReader(pdf_path)
        full_text = ""
        for page_num, page in enumerate(reader.pages, 1):
            if self.max_pages is not None and page_num > int(self.max_pages):
                break
            try:
                page_text = page.extract_text()
                if page_text:
                    full_text += f"\n--- Page {page_num} ---\n{page_text}\n"
            except Exception as e:
                print(f"Warning: could not extract page {page_num}: {e}")
        return full_text.strip()

    def txt_to_text(self, txt_path: Path) -> str:
        with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
        if self.max_chars is not None:
            return raw[: int(self.max_chars)]
        return raw

    def process_pdf(self, pdf_path: Path) -> List[Dict]:
        text = self.pdf_to_text(pdf_path)
        if not text.strip():
            print(f"Warning: no text extracted from {pdf_path.name}")
            return []
        metadata = {"source": pdf_path.name, "source_type": "pdf", "file_path": str(pdf_path)}
        return self.chunk_text(text, metadata)

    def process_txt(self, txt_path: Path) -> List[Dict]:
        text = self.txt_to_text(txt_path)
        if not text.strip():
            return []
        metadata = {"source": txt_path.name, "source_type": "txt", "file_path": str(txt_path)}
        return self.chunk_text(text, metadata)

