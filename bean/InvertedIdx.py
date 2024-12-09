from Util.Container import Container
from Util.DeltaEncoder import DeltaEncoder
import Util.index_batch as ib
import Util.simhashdetect as shd

class InvertedIndex:
    def __init__(self):
        self.container = Container()
        self.encoder = DeltaEncoder()
        self.total_doc = 0
        self.batch_id = 0
        self.duplication = 0

    def add_document(self, document_id, tokens, anchor_words):

        if shd.is_duplicate_list(tokens):
            self.duplication += 1
            print("Duplicate: " + str(self.duplication))
            return

        term_frequency = {}
        positions = {}
        self.total_doc += 1
        print(document_id)

        for index, token in enumerate(tokens):
            if token not in term_frequency:
                term_frequency[token] = 0
                positions[token] = []
            term_frequency[token] += 1
            positions[token].append(index)

        for term, frequency in term_frequency.items():
            sorted_positions = sorted(positions[term])
            if term in anchor_words:
                self.container.add_posting(term, document_id, frequency, sorted_positions,1)
            else:
                self.container.add_posting(term, document_id, frequency, sorted_positions, 0)

    def get_postings(self, term):
        return self.container.search(term)

    def get_all_terms(self):
        return self.container.get_all_terms()

    def save_into_batch(self, index_terms):
        self.batch_id += 1
        inverted_index_batch = self.container.get_all_terms()
        ib.process_and_save_batches(inverted_index_batch, index_terms, self.batch_id)
        self.container.clear()
