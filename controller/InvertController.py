from bean import InvertedIdx
from controller import Token

def start_invert_index(doc_ids, tokens):
    inverted_index = InvertedIdx.InvertedIndex()
    for i in range(len(doc_ids)):
        inverted_index.add_document(doc_ids[i], tokens[i])
    return inverted_index

def get_postings(inverted_index, term):
    return inverted_index.get_postings(term)

# 调用Token.build_from_json_files 开始构建倒排索引
def start(folder_path, inverted_index):
    Token.build_from_json_files(folder_path, inverted_index)
    # create bitmap
    # for term, postings in inverted_index.container.dict.items():
    #     doc_ids = [posting[0] for posting in postings]
    #     inverted_index.container.create_bitmap(term, doc_ids, inverted_index.total_doc)
    #
    # inverted_index.container.rle_encode_all_bitmap()





