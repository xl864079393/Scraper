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

    
    def process_document(self, document_id, json_content):
        # parse JSON
        data = json.loads(json_content)
        
        # extract text fields (assuming 'title', 'body', and 'tags' fields)
        content = data.get("content", "")
        
        removed_wordlist = ['pptx', 'html', 'pdf', 'docx', 'doc', 
                            'ppt', 'xls', 'xlsx', 'txt', 'csv', 
                            'jpg', 'jpeg', 'png', 'gif', 'bmp', 
                            'tiff', 'svg', 'mp4', 'avi', 'mov', 
                            'wmv', 'flv', 'mp3', 'wav', 'wma', 
                            'aac', 'flac', 'ogg', 'zip', 'rar', 
                            '7z', 'tar', 'gz', 'bz2', 'xz', 'pdf', 
                            'exe', 'msi', 'apk', 'dmg', 'iso', 'img', 
                            'bin', 'cue', 'mdf', 'mds', 'nrg', 'vcd', 
                            'torrent', 'url', 'html', 'xml', 'json', 
                            'css', 'js', 'php', 'asp', 'jsp', 'py', 
                            'java', 'c', 'cpp', 'h', 'hpp', 'cs', 
                            'vb', 'vbs', 'bat', 'sh', 'ps1', 'psm1', 
                            'psd1', 'ps1xml', 'psc1', 'pssc', 'msh', 'msh1', 'msh2', 'mshxml']
        # tokenize and stem
        tokens = self.tokenize(content)
        for token in tokens[:]:
            if token.isdigit():
                tokens.remove(token)
                continue
            if any(char.isdigit() for char in token) and any(char.isalpha() for char in token):
                tokens.remove(token)
                continue
            if token in removed_wordlist:
                tokens.remove(token)
                continue
            if len(token) == 1:
                tokens.remove(token)
                continue
        print(document_id)

        word_count = len(tokens)
        return(document_id, tokens, word_count)

        '''
        ps = PorterStemmer()
        stemmed_tokens = [ps.stem(token) for token in tokens]

        # calculate term frequencies
        term_counts = Counter(stemmed_tokens)
        total_terms = sum(term_counts.values())
        
        # update document metadata, doc_length link to file_name
        self.doc_lengths[num] = total_terms
        
        # add to index
        for term in term_counts.items():
            self.index[term].append({"file_name": num})
        '''
    # open file and read json content
    
    def build_from_json_files(self, folder_path):
        # compute the length of the folder path
        num = 1
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".json"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, "r", encoding="utf-8") as file:
                    json_content = file.read()
                    self.process_document(num, json_content)
                    num += 1
    
    '''
    def build_from_json_files(self, file_name):
        #for file_name in os.listdir(folder_path):
        num = 1
        if file_name.endswith(".json"):
            #file_path = os.path.join(folder_path, file_name)
            with open(file_name, "r", encoding="utf-8") as file:
                json_content = file.read()
                self.process_document(num, json_content)
    '''
    
    def display_index(self):
        for term, postings in self.index.items():
            print(f"{term}: {postings}")

folder_path = "/Users/zhengxuanli/Downloads/ANALYST/www_cs_uci_edu"
index = InvertedIndex()
index.build_from_json_files(folder_path)
# index.display_index()