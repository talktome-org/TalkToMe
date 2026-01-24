import asyncio
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
PARENT = ROOT.parent
load_dotenv(dotenv_path=ROOT / ".env")

# Add repo root so "Backend" package imports work
if str(PARENT) not in sys.path:
    sys.path.insert(0, str(PARENT))

from Backend.Services.document_service import DocumentProcessor
from Backend.Services.embedding_service import EmbeddingService
from Backend.crud.books.rag_crud import delete_chunks_by_source, insert_chunks_batch, upsert_document


def _iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _split_txt_file(path: Path, *, split_lines: int, tmp_dir: Path) -> list[Path]:
    """
    Split a large TXT into smaller parts by line count to reduce memory.
    Returns list of part file paths.
    """
    parts_dir = tmp_dir / "splits" / path.stem
    parts_dir.mkdir(parents=True, exist_ok=True)

    part_paths: list[Path] = []
    part_idx = 0
    line_idx = 0
    out_f = None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if out_f is None or line_idx >= split_lines:
                    if out_f:
                        out_f.close()
                    part_idx += 1
                    line_idx = 0
                    part_path = parts_dir / f"{path.stem}_part_{part_idx:04d}.txt"
                    out_f = open(part_path, "w", encoding="utf-8")
                    part_paths.append(part_path)
                out_f.write(line)
                line_idx += 1
    finally:
        if out_f:
            out_f.close()
    return part_paths


async def ingest_books(
    *,
    books_dir: Path | None = None,
    reingest: bool = False,
    audience: str | None = None,
    tags: list[str] | None = None,
    embedding_model: str = "text-embedding-3-small",
    chunk_size: int = 500,
    chunk_overlap: int = 100,
    max_pages: int | None = None,
    max_chars: int | None = None,
    batch_size: int = 10,
    max_chunks: int | None = None,
    stage: str = "all",
    tmp_dir: Path | None = None,
    chunker: str = "tokens",
    split_lines: int | None = None,
):
    if books_dir is None:
        books_dir = Path(__file__).resolve().parent.parent / "books"
    books_dir.mkdir(exist_ok=True)

    processor = DocumentProcessor(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        max_pages=max_pages,
        max_chars=max_chars,
        use_tokenizer=(chunker == "tokens"),
    )
    embedder = EmbeddingService(model=embedding_model)
    if tmp_dir is None:
        tmp_dir = ROOT / "tmp_rag"

    pdfs = list(books_dir.glob("*.pdf"))
    txts = list(books_dir.glob("*.txt"))
    files = pdfs + txts

    if not files:
        print(f"No PDFs/TXTs found in {books_dir}")
        return

    total_inserted = 0

    for file_path in files:
        print(f"\n=== Processing {file_path.name} ===")
        try:
            # For large TXT files, optionally split into smaller parts
            part_files: list[Path] = []
            if file_path.suffix.lower() == ".txt" and split_lines:
                part_files = _split_txt_file(file_path, split_lines=split_lines, tmp_dir=tmp_dir)
            else:
                part_files = [file_path]

            global_chunk_index = 0

            for part_path in part_files:
                if part_path.suffix.lower() == ".pdf":
                    chunks = processor.process_pdf(part_path)
                else:
                    chunks = processor.process_txt(part_path)

                if not chunks:
                    print(f"No chunks created for {part_path.name}; skipping.")
                    continue

                if max_chunks is not None and len(chunks) > max_chunks:
                    chunks = chunks[:max_chunks]

                # Stage 1: Chunk -> JSONL
                if stage in ("chunk", "all", "embed", "insert"):
                    chunk_rows = []
                    for chunk in chunks:
                        meta = chunk.get("metadata", {}) or {}
                        # enforce globally unique chunk_index per source
                        meta["chunk_index"] = global_chunk_index
                        global_chunk_index += 1
                        if audience:
                            meta["audience"] = audience
                        if tags:
                            meta["tags"] = tags
                        if part_path != file_path:
                            meta["part"] = part_path.name
                        chunk_rows.append({"text": chunk["text"], "metadata": meta})

                    chunks_path = tmp_dir / f"{file_path.stem}.chunks.jsonl"
                    _write_jsonl(chunks_path, chunk_rows)
                    print(f"Wrote chunks to {chunks_path}")

                # Stage 2: Embed -> JSONL
                if stage in ("embed", "all", "insert"):
                    chunks_path = tmp_dir / f"{file_path.stem}.chunks.jsonl"
                    if not chunks_path.exists():
                        print(f"Missing chunks file: {chunks_path}. Run with --stage chunk first.")
                        continue

                    embedded_path = tmp_dir / f"{file_path.stem}.embeddings.jsonl"
                    with open(embedded_path, "w", encoding="utf-8") as out_f:
                        batch_rows = []
                        batch_texts = []
                        for row in _iter_jsonl(chunks_path):
                            text = row.get("text") or ""
                            if not text.strip():
                                continue
                            batch_rows.append(row)
                            batch_texts.append(text)
                            if len(batch_rows) >= batch_size:
                                embeddings = embedder.get_embeddings_batch(batch_texts, batch_size=batch_size)
                                for r, e in zip(batch_rows, embeddings):
                                    out_f.write(json.dumps({**r, "embedding": e, "embedding_model": embedding_model}, ensure_ascii=False) + "\n")
                                batch_rows, batch_texts = [], []
                        if batch_rows:
                            embeddings = embedder.get_embeddings_batch(batch_texts, batch_size=batch_size)
                            for r, e in zip(batch_rows, embeddings):
                                out_f.write(json.dumps({**r, "embedding": e, "embedding_model": embedding_model}, ensure_ascii=False) + "\n")
                    print(f"Wrote embeddings to {embedded_path}")

                # Stage 3: Insert into Supabase
                if stage in ("insert", "all"):
                    embedded_path = tmp_dir / f"{file_path.stem}.embeddings.jsonl"
                    if not embedded_path.exists():
                        print(f"Missing embeddings file: {embedded_path}. Run with --stage embed first.")
                        continue

                    if reingest:
                        deleted = await delete_chunks_by_source(source=file_path.name)
                        if deleted:
                            print(f"Deleted {deleted} existing chunks for {file_path.name}")

                    document_id, _ = await upsert_document(
                        source_type=file_path.suffix.lower().lstrip("."),
                        source=file_path.name,
                        title=file_path.stem,
                        metadata={"audience": audience, "tags": tags or []},
                    )

                    batch_chunks = []
                    batch_embeddings = []
                    for row in _iter_jsonl(embedded_path):
                        meta = row.get("metadata", {}) or {}
                        meta["document_id"] = str(document_id)
                        batch_chunks.append({"text": row.get("text") or "", "metadata": meta})
                        batch_embeddings.append(row.get("embedding"))
                        if len(batch_chunks) >= batch_size:
                            inserted = await insert_chunks_batch(
                                document_id=document_id,
                                chunks=batch_chunks,
                                embeddings=batch_embeddings,
                                embedding_model=embedding_model,
                            )
                            total_inserted += inserted
                            batch_chunks, batch_embeddings = [], []
                    if batch_chunks:
                        inserted = await insert_chunks_batch(
                            document_id=document_id,
                            chunks=batch_chunks,
                            embeddings=batch_embeddings,
                            embedding_model=embedding_model,
                        )
                        total_inserted += inserted
                    print(f"Inserted chunks for {file_path.name} into Supabase.")
        except Exception as e:
            print(f"Error processing {file_path.name}: {e}")

    print(f"\nDone. Total inserted: {total_inserted}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest therapy PDFs/TXTs into Supabase vector table.")
    parser.add_argument("--books-dir", type=str, help="Directory containing PDFs/TXTs")
    parser.add_argument("--reingest", action="store_true", help="Delete existing chunks for each source before ingesting")
    parser.add_argument("--audience", type=str, help="Audience label to store in metadata (e.g., man, woman, child)")
    parser.add_argument("--audience-tags", type=str, help="Comma-separated tags for metadata (e.g., teen,parent)")
    parser.add_argument("--embedding-model", type=str, default="text-embedding-3-small", help="Embedding model to use")
    parser.add_argument("--chunk-size", type=int, default=500, help="Token chunk size")
    parser.add_argument("--chunk-overlap", type=int, default=100, help="Token overlap between chunks")
    parser.add_argument("--max-pages", type=int, default=None, help="Max pages to read from PDF")
    parser.add_argument("--max-chars", type=int, default=None, help="Max characters to read from TXT")
    parser.add_argument("--batch-size", type=int, default=10, help="Embedding batch size")
    parser.add_argument("--max-chunks", type=int, default=None, help="Max chunks per file (truncate)")
    parser.add_argument("--stage", type=str, default="all", choices=["chunk", "embed", "insert", "all"], help="Pipeline stage to run")
    parser.add_argument("--tmp-dir", type=str, default=None, help="Directory for intermediate chunk/embed files")
    parser.add_argument("--chunker", type=str, default="tokens", choices=["tokens", "chars"], help="Chunk by tokens or characters")
    parser.add_argument("--split-lines", type=int, default=None, help="Split TXT files into parts by line count")
    args = parser.parse_args()

    books_dir = Path(args.books_dir) if args.books_dir else None
    tags = args.audience_tags.split(",") if args.audience_tags else None
    asyncio.run(
        ingest_books(
            books_dir=books_dir,
            reingest=args.reingest,
            audience=args.audience,
            tags=tags,
            embedding_model=args.embedding_model,
            chunk_size=args.chunk_size,
            chunk_overlap=args.chunk_overlap,
            max_pages=args.max_pages,
            max_chars=args.max_chars,
            batch_size=args.batch_size,
            max_chunks=args.max_chunks,
            stage=args.stage,
            tmp_dir=Path(args.tmp_dir) if args.tmp_dir else None,
            chunker=args.chunker,
            split_lines=args.split_lines,
        )
    )


