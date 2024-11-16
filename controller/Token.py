import os
import json
from collections import defaultdict, Counter
from bean.InvertedIdx import InvertedIndex
import re


def tokenize(text):
    # Tokenize and clean text
    return re.findall(r'\b\w+\b', text.lower())


def process_document(document_id, json_content):
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
    tokens = tokenize(content)
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

    word_count = len(tokens)
    return document_id, tokens, word_count


# open file and read json content
def build_from_json_files(folder_path, inverted_index):
    # compute the length of the folder path
    num = 1
    for folder_name in os.listdir(folder_path):
        for file_name in os.listdir(os.path.join(folder_path, folder_name)):
            if num % 150 == 0:
                print("Initiating all data")
                inverted_index.Init_all_data()
            if file_name.endswith(".json"):
                file_path = os.path.join(folder_path,folder_name, file_name)
                with open(file_path, "r", encoding = "utf-8") as file:
                    json_content = file.read()
                    inverted_index.add_document(*process_document(num, json_content))
                    num += 1
    inverted_index.Init_all_data()