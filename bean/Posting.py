class Posting:
    def __init__(self, document_id, term_frequency, positions, doc_length):
        self.document_id = document_id  # 文档ID
        self.term_frequency = term_frequency  # 词频
        self.positions = positions  # 词项的位置列表
        self.doc_length = doc_length  # 文档长度

    def __repr__(self):
        return f"Posting(Document ID: {self.document_id}, TF: {self.term_frequency}, " \
               f"Positions: {self.positions}, Doc Length: {self.doc_length})"
