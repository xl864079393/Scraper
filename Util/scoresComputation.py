import math

def binary_search(sorted_list, target):
    left, right = 0, len(sorted_list) - 1
    while left <= right:
        mid = (left + right) // 2
        if sorted_list[mid][0] == target:
            return mid
        elif sorted_list[mid][0] < target:
            left = mid + 1
        else:
            right = mid - 1
    return -1  # 未找到目标值

def compute_tf_idf(N, terms_dict, intersection_docs):  # 总文档数
    scores = {}

    for term in terms_dict:
        term_data = terms_dict[term] # 获取当前词的倒排索引信息
        df = len(term_data)  # 包含该词的文档数量
        idf = math.log(N / (df + 1))  # 防止除以 0

        for doc_id in intersection_docs:
            find = binary_search(term_data, doc_id)
            if find == -1:
                continue
            tf = term_data[find][1]  # 当前文档的词频
            tf_idf = tf * idf  # 计算 TF-IDF
            scores[doc_id] = scores.get(doc_id, 0) + tf_idf  # 累加多个词的 TF-IDF
    return scores