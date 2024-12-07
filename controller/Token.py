import os
import json
import re
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
import gc
import time
import pickle


# tokenization and stemming
def process_document(document_id, json_content):
    ps = PorterStemmer()

    def extract_and_stem(text):
        # Tokenize the text and convert it to lowercase
        tokens = re.findall(r'\b\w+\b', text.lower())
        # Stem the tokens
        return [ps.stem(token) for token in tokens]

    def generate_trigrams(tokens):
        # Create trigrams (sequences of three consecutive words)
        trigrams = [tuple(tokens[i:i+3]) for i in range(len(tokens) - 2)]
        return trigrams

    if not isinstance(json_content, dict) or json_content["content"] is None:
        raise ValueError("Input JSON must contain a 'content' field.")

    content = json_content["content"]
    soup = BeautifulSoup(content, "html.parser")
    body_text = soup.get_text()

    # Extract and stem the tokens
    tokens = extract_and_stem(body_text)

    # Generate trigrams from tokens
    trigrams = generate_trigrams(tokens)

    # Combine unigrams and trigrams
    all_terms = tokens + [' '.join(trigram) for trigram in trigrams]

    # Return both unigrams and trigrams for indexing/search
    return document_id, all_terms


# 被InvertController.start调用
# 遍历文件夹中的所有json文件，将每个文件的内容提取出来进行 process_document()，然后调用InvertedIndex.add_document方法
def build_from_json_files(folder_path, inverted_index):
    index_bookkeeping = {}
    docid_dict = {}
    num = 1
    all_files = [
        os.path.join(folder_path, folder_name, file_name)
        for folder_name in os.listdir(folder_path)
        for file_name in os.listdir(os.path.join(folder_path, folder_name))
        if file_name.endswith(".json")
    ]

    for file_path in all_files:
        with open(file_path, "r", encoding = "utf-8") as file:
            json_content = file.read()

        json_content = json.loads(json_content)

        document = process_document(num, json_content)

        if document:
            inverted_index.add_document(*document)

        if num % 3000 == 0:
            if len(inverted_index.container.dict) > 2000000:
                print("Total term:" + str(len(inverted_index.container.dict)))
                start_time = time.time()
                inverted_index.save_into_batch(index_bookkeeping)
                end_time = time.time()
                print(f"Total Time: {end_time - start_time}")
                gc.collect()


        docid_dict[num] = json_content["url"]
        num += 1

    with open("docid_dict.json", "w", encoding="utf-8") as f:
        json.dump(docid_dict, f, ensure_ascii=False)

    print("Total term:" + str(len(inverted_index.container.dict)))
    start_time = time.time()
    inverted_index.save_into_batch(index_bookkeeping)
    end_time = time.time()
    print(f"Total Time: {end_time - start_time}")
    gc.collect()

    try:
        with open("index_bookkeeping.pkl", "wb") as f:
            pickle.dump(index_bookkeeping, f)
    except Exception as e:
        print(e)
        print("Error saving bookkeeping file.")


