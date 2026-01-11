from pathlib import Path
from typing import Dict, List

import tiktoken
from pypdf import PdfReader


class DocumentProcessor:
    """
    Handles PDF/TXT ingestion:
    - Extract text
    - Tokenize and chunk with overlap
    """

    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.encoding = tiktoken.get_encoding("cl100k_base")

    def count_tokens(self, text: str) -> int:
        return len(self.encoding.encode(text))

    def chunk_text(self, text: str, metadata: Dict = None) -> List[Dict]:
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

    def pdf_to_text(self, pdf_path: Path) -> str:
        reader = PdfReader(pdf_path)
        full_text = ""
        for page_num, page in enumerate(reader.pages, 1):
            try:
                page_text = page.extract_text()
                if page_text:
                    full_text += f"\n--- Page {page_num} ---\n{page_text}\n"
            except Exception as e:
                print(f"Warning: could not extract page {page_num}: {e}")
        return full_text.strip()

    def txt_to_text(self, txt_path: Path) -> str:
        with open(txt_path, "r", encoding="utf-8") as f:
            return f.read()

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

