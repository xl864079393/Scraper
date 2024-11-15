from Util.Trie import Trie
from Util.DeltaEncoder import DeltaEncoder
from bean.Posting import Posting
import json

class InvertedIndex:
    def __init__(self):
        self.trie = Trie()
        self.encoder = DeltaEncoder()

    # def _serialize(self, node):
    #     """将trie节点数据序列化为可存储的字典格式"""
    #     if not node:
    #         return None
    #     node_data = {}
    #     for char, child_node in node.children.items():
    #         node_data[char] = self._serialize(child_node)
    #     return {'children': node_data, 'is_end_of_word': node.is_end_of_word}
    #
    # def backup_to_file(self, filename = "inverted_index_backup.json"):
    #     """Backup the inverted index to a file in a compact format."""
    #     with open(filename, 'w') as f:
    #         serialized_data = self._serialize(self.trie.root)
    #         f.write(json.dumps(serialized_data))

    def add_document(self, document_id, tokens, doc_length):
        term_frequency = {}
        positions = {}

        for index, token in enumerate(tokens):
            if token not in term_frequency:
                term_frequency[token] = 0
                positions[token] = []
            term_frequency[token] += 1
            positions[token].append(index)

        for term, frequency in term_frequency.items():
            sorted_positions = sorted(positions[term])
            self.trie.add_posting(term, document_id, frequency, sorted_positions, doc_length)

    def get_postings(self, term):
        postings, doc_encoder = self.trie.search(term)
        result = []
        if postings is not None:
            doc_decoder = self.encoder.decode(doc_encoder)
            for i in range(len(postings)):
                posting = Posting(doc_decoder[i], postings[i][1], self.encoder.decode(postings[i][2]), postings[i][3])
                result.append(posting)
            return result
        return []

    def get_all_terms(self):
        return self.trie.get_all_terms()

    def get_raw_postings(self, term):
        postings, _ = self.trie.search(term)
        return {term: postings}
