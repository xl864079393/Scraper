from Util.DeltaEncoder import DeltaEncoder
from collections import defaultdict
from bitarray import bitarray


class Container:
    def __init__(self):
        self.dict = defaultdict(list)
        self.encoder = DeltaEncoder()
        self.bitmap = {}

    def insert(self, word, posting):
        if word in self.dict:
            self.dict[word].append(posting)
        else:
            self.dict[word] = [posting]

    def search(self, word):
        return self.dict.get(word, [])

    # def _delta_encode_postings(self, postings):
    #     if not postings:
    #         return [], []
    #     doc_encoder = self.encoder.encode([p[0] for p in postings])
    #     encoded_postings = [
    #         (postings[i][0], postings[i][1], postings[i][2], postings[i][3])
    #         for i in range(len(postings))
    #     ]
    #
    #     return encoded_postings, doc_encoder

    def create_bitmap(self, word, doc_ids, total_docs):
        bitmap = bitarray(total_docs+1)
        bitmap.setall(0)

        for doc_id in doc_ids:
            bitmap[doc_id] = 1

        self.bitmap[word] = bitmap

    def add_posting(self, term, document_id, term_frequency, positions):
        posting = (document_id, term_frequency, positions)
        self.insert(term, posting)
        #node.index, node.doc_encoder = self._delta_encode_postings(node.index)

    # def rle_encode_all_bitmap(self):
    #     for key in self.bitmap.keys():
    #         self.bitmap[key] = rle_encode(self.bitmap[key])


    def get_all_terms(self):
        return self.dict

    def clear(self):
        self.dict.clear()