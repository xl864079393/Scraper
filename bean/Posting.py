class Posting:
    def __init__(self, document_id, term_frequency, positions, doc_length):
        self.document_id = document_id
        self.term_frequency = term_frequency
        self.positions = positions
        self.doc_length = doc_length

    def __repr__(self):
        return f"Posting(Document ID: {self.document_id}, TF: {self.term_frequency}, " \
               f"Positions: {self.positions}, Doc Length: {self.doc_length})"
