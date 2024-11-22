from Util.Container import Container
from Util.DeltaEncoder import DeltaEncoder
from bean.Posting import Posting
from collections import defaultdict
import os
import gc
import json

class InvertedIndex:
    def __init__(self):
        self.container = Container()
        self.encoder = DeltaEncoder()
        self.total_doc = 0

    def add_document(self, document_id, tokens):
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
            self.container.add_posting(term, document_id, frequency, [sorted_positions[0], sorted_positions[-1]])

    def get_postings(self, term):
        return self.container.search(term)

    def get_all_terms(self):
        return self.container.get_all_terms()

    def Init_all_data(self, target_file="temp/target.json"):
        if os.path.exists(target_file):
            with open(target_file, 'r', encoding="utf-8") as f:
                try:
                    existData = defaultdict(list, json.load(f))
                except json.JSONDecodeError:
                    existData = defaultdict(list)
        else:
            existData = defaultdict(list)

        currentData = self.get_all_terms()

        for terms, postings in currentData.items():
            existData[terms].extend(postings)
        self.container.clear()
        with open(target_file, 'w', encoding="utf-8") as f:
            json.dump(dict(existData), f, ensure_ascii=False)
        print("Total doc: ", self.total_doc)
        print("Total term: ", len(existData))
        gc.collect()
