class DeltaEncoder:
    def __init__(self):
        pass

    def encode(self, data):
        if not data:
            return []

        encoded_data = [data[0]]  # 第一个元素原样存储
        for i in range(1, len(data)):
            encoded_data.append(data[i] - data[i - 1])  # 存储与前一个元素的差值
        return encoded_data

    def decode(self, encoded_data):
        if not encoded_data:
            return []

        decoded_data = [encoded_data[0]]  # 第一个元素原样存储
        for i in range(1, len(encoded_data)):
            decoded_data.append(decoded_data[-1] + encoded_data[i])  # 累加差值得到原始数据

        return decoded_data

    # def encode_postings(self, postings):
    #     if not postings:
    #         return [], []
    #     doc_encoder = self.encode([p[0] for p in postings])
    #     encoded_postings = [
    #         (postings[i][0], postings[i][1], postings[i][2], postings[i][3])
    #         for i in range(len(postings))
    #     ]
    #     return encoded_postings, doc_encoder