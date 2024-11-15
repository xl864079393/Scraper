import os
import json
from nltk.stem import PorterStemmer
from collections import defaultdict, Counter
import re

class InvertedIndex:
    def __init__(self):
        self.index = defaultdict(list)  # Token to postings
        self.doc_lengths = {}  # Document ID to word count
    
    def tokenize(self, text):
        # Tokenize and clean text
        return re.findall(r'\b\w+\b', text.lower())
    
    def process_document(self, doc_id, json_content):
        # parse JSON
        data = json.loads(json_content)
        
        # extract text fields (assuming 'title', 'body', and 'tags' fields)
        content = data.get("content", "")
        
        # tokenize and stem
        tokens = self.tokenize(content)
        print(tokens)
        ps = PorterStemmer()
        stemmed_tokens = [ps.stem(token) for token in tokens]

        # calculate term frequencies
        term_counts = Counter(stemmed_tokens)
        total_terms = sum(term_counts.values())
        
        # update document metadata
        self.doc_lengths[doc_id] = total_terms
        
        # add to index
        for term, count in term_counts.items():
            tf = count / total_terms
            positions = [i for i, t in enumerate(stemmed_tokens) if t == term]
            self.index[term].append({"doc_id": doc_id, "tf": tf, "positions": positions})
    
    def build_from_json_files(self, file_name):
        #for file_name in os.listdir(folder_path):
        if file_name.endswith(".json"):
            #file_path = os.path.join(folder_path, file_name)
            with open(file_name, "r", encoding="utf-8") as file:
                json_content = file.read()
                self.process_document(file_name, json_content)
    
    def display_index(self):
        for term, postings in self.index.items():
            print(f"{term}: {postings}")

folder_path = "/Users/zhengxuanli/Downloads/ANALYST/www_cs_uci_edu/0a77b224f19e2fadc0ec26a19e7b6219dc56833f005fbd658f6eb8194804883e.json"
index = InvertedIndex()
index.build_from_json_files(folder_path)
index.display_index()