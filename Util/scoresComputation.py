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

def compute_HITS_PR(intersection_docs ,page_rank_scores, authority_scores, scores, alpha=0.5, beta=0.3, gamma=0.2):
    # Normalize PageRank and Authority scores
    max_pr = max(page_rank_scores.values()) if page_rank_scores else 1
    max_auth = max(authority_scores.values()) if authority_scores else 1

    for doc_id in intersection_docs:
        if doc_id not in page_rank_scores or doc_id not in authority_scores:
            continue
        pr_score = page_rank_scores.get(doc_id, 0) / max_pr  # Normalize PageRank
        auth_score = authority_scores.get(doc_id, 0) / max_auth  # Normalize Authority

        # Combine TF-IDF, PageRank, and Authority scores
        scores[doc_id] = (
                alpha * scores.get(doc_id, 0) + beta * pr_score + gamma * auth_score
        )

    return scores

def compute_proximity_position(intersection_docs, terms_dict, max_distance, scores, alpha=0.2):
    for doc_id in intersection_docs:
    # Retrieve the positions of each query term in the document
        positions = []
        for term in terms_dict:
            term_data = terms_dict[term]
            find = binary_search(term_data, doc_id)
            if find != -1:
                position = term_data[find][2]
                positions.append(position)


        # If any term is not found in the document, skip it
        if any(len(pos_list) == 0 for pos_list in positions):
            continue
        # Calculate proximity score for this document based on term positions
        proximity_score = 0
        for i, pos_list_1 in enumerate(positions[:-1]):
            for pos_1 in pos_list_1:
                # Compare with all positions of the next term
                for pos_2 in positions[i + 1]:
                    if abs(pos_1 - pos_2) <= max_distance:
                        proximity_score += 1  # Increment score if terms are within the max distance
        if proximity_score > 0:
            proximity_score = math.log(proximity_score + 1)

        scores[doc_id] = scores.get(doc_id, 0) + alpha * proximity_score

def compute_anchor(intersection_docs, terms_dict, scores, alpha=1):
    for doc_id in intersection_docs:
        for term in terms_dict:
            term_data = terms_dict[term]
            find = binary_search(term_data, doc_id)
            if find != -1:
                anchor = term_data[find][3]
                if anchor == 1:
                    scores[doc_id] = scores.get(doc_id, 0) + alpha
    return scores