from Util.DeltaEncoder import DeltaEncoder
from bean.Posting import Posting
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.index = []
        self.doc_encoder = []


class Trie:
    def __init__(self):
        self.root = TrieNode()
        self.encoder = DeltaEncoder()

    def insert(self, word, posting):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True
        node.index.append(posting)

    def search(self, word):
        node = self.root
        for char in word:
            if char not in node.children:
                return None
            node = node.children[char]
        return node.index, node.doc_encoder if node.is_end_of_word else None

    def _delta_encode_postings(self, postings):
        if not postings:
            return [], []
        doc_encoder = self.encoder.encode([p[0] for p in postings])  # 对文档ID差分编码
        encoded_postings = [
            (postings[i][0], postings[i][1], postings[i][2], postings[i][3])
            for i in range(len(postings))
        ]

        return encoded_postings, doc_encoder

    def add_posting(self, term, document_id, term_frequency, positions, doc_length):
        posting = (document_id, term_frequency, self.encoder.encode(positions), doc_length)
        node = self.root
        for char in term:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]

        node.is_end_of_word = True
        node.index.append(posting)
        node.index, node.doc_encoder = self._delta_encode_postings(node.index)

    # return all terms with their postings
    def get_all_terms(self):
        def _get_terms(node, term, terms):
            if node.is_end_of_word:
                terms[term] = node.index
            for char, child_node in node.children.items():
                _get_terms(child_node, term + char, terms)

        terms = {}
        _get_terms(self.root, '', terms)
        return terms

    # store all terms with their postings to a dictionary
    def store_all_terms(self, target_dict):
        terms = self.get_all_terms()
        for term, postings in terms.items():
            target_dict[term] = postings
        return terms

    # clear all terms
    def clear(self):
        self.root = TrieNode()
        self.encoder = DeltaEncoder()
        return self.root