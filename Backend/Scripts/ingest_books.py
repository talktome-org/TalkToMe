#!/usr/bin/env python3
"""
PDF/TXT -> text -> chunks -> embeddings -> Supabase
Place files in Backend/Books/ then run:
python Scripts/ingest_books.py --reingest   # optional flag deletes old rows for each source
"""

import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=ROOT / ".env")

sys.path.insert(0, str(ROOT))

from Services.document_processor import DocumentProcessor
from Services.embedding_service import EmbeddingService
from Database.rag_repo import insert_chunks_batch, delete_chunks_by_source, upsert_document


async def ingest_books(
    *,
    books_dir: Path | None = None,
    reingest: bool = False,
    audience: str | None = None,
    tags: list[str] | None = None,
    embedding_model: str = "text-embedding-3-small",
):
    if books_dir is None:
        books_dir = Path(__file__).resolve().parent.parent / "Books"
    books_dir.mkdir(exist_ok=True)

    processor = DocumentProcessor(chunk_size=1000, chunk_overlap=200)
    embedder = EmbeddingService(model=embedding_model)

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
            if file_path.suffix.lower() == ".pdf":
                chunks = processor.process_pdf(file_path)
            else:
                chunks = processor.process_txt(file_path)

            if not chunks:
                print("No chunks created; skipping.")
                continue

            if reingest:
                deleted = await delete_chunks_by_source(source=file_path.name)
                if deleted:
                    print(f"Deleted {deleted} existing chunks for {file_path.name}")

            # Upsert document record (unique by source_type + source)
            document_id, _ = await upsert_document(
                source_type=file_path.suffix.lower().lstrip("."),
                source=file_path.name,
                title=file_path.stem,
                metadata={"audience": audience, "tags": tags or []},
            )

            # Attach doc metadata for each chunk
            for chunk in chunks:
                meta = chunk.get("metadata", {}) or {}
                meta["document_id"] = str(document_id)
                if audience:
                    meta["audience"] = audience
                if tags:
                    meta["tags"] = tags
                chunk["metadata"] = meta

            texts = [c["text"] for c in chunks]
            print(f"Creating embeddings for {len(texts)} chunks...")
            embeddings = embedder.get_embeddings_batch(texts, batch_size=100)

            inserted = await insert_chunks_batch(
                document_id=document_id,
                chunks=chunks,
                embeddings=embeddings,
                embedding_model=embedding_model,
            )
            print(f"Inserted {inserted} chunks into Supabase.")
            total_inserted += inserted
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
        )
    )

