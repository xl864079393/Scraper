import math

def compute_tf_idf(data, terms_dict, intersection_docs):
    N = len(data)  # 总文档数
    scores = {}

    for term in terms_dict:
        term_data = terms_dict[term] # 获取当前词的倒排索引信息
        df = len(term_data)  # 包含该词的文档数量
        idf = math.log(N / (df + 1))  # 防止除以 0
        print(f"Term: {term}, DF: {df}, IDF: {idf}")

        for doc_id in intersection_docs:
            tf = term_data[doc_id]  # 当前文档的词频
            print(f"Doc ID: {doc_id}, TF: {tf}")
            tf_idf = tf * idf  # 计算 TF-IDF
            scores[doc_id] = scores.get(doc_id, 0) + tf_idf  # 累加多个词的 TF-IDF

    return scores