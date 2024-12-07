import pyroaring

def create_bitmap(word, doc_ids, bitmaps):
    bitmap = pyroaring.BitMap(doc_ids)
    bitmaps[word] = bitmap

def preprocess_bitmap(bitmaps, data):
    for term, postings in data.items():
        doc_ids = [posting[0] for posting in postings]
        create_bitmap(term, doc_ids, bitmaps)