from bean import InvertedIdx

def start_invert_index(doc_ids, tokens, doc_lengths):
    inverted_index = InvertedIdx.InvertedIndex()
    for i in range(len(doc_ids)):
        inverted_index.add_document(doc_ids[i], tokens[i], doc_lengths[i])
    return inverted_index

def get_postings(inverted_index, term):
    return inverted_index.get_postings(term)



# inverted_index = InvertedIdx.InvertedIndex()
#
# doc1 = 1  # 文档ID
# tokens1 = ["apple", "banana", "apple", "apple"]
# doc_length1 = len(tokens1)  # 计算文档长度（单词数）
#
# doc2 = 2  # 文档ID
# tokens2 = ["apple", "orange", "apple", "banana"]
# doc_length2 = len(tokens2)
#
# # 添加文档到倒排索引
# inverted_index.add_document(doc1, tokens1, doc_length1)
# inverted_index.add_document(doc2, tokens2, doc_length2)
#
# # 查询某个词项的倒排列表（已经进行差分编码）
# postings_apple = inverted_index.get_postings("apple")
# postings_banana = inverted_index.get_postings("banana")
#
# print("Postings for 'apple':", postings_apple)
# print("Postings for 'banana':", postings_banana)


