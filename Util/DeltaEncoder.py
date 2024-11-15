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