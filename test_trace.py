from pathlib import Path
import sys
sys.path.insert(0,'/Users/mac/Desktop/TherAI/Backend')
from Services.document_processor import DocumentProcessor
proc = DocumentProcessor(chunk_size=800, chunk_overlap=160, max_pages=5)
chunks = proc.process_pdf(Path('/Users/mac/Desktop/TherAI/Backend/Books/SecretsAboutMenPDF.pdf'))
print('chunks', len(chunks))
