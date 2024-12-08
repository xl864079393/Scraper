# import hashli


# def compute_simhash(text, hash_bits=64):
#     tokens = text.split()
#     hash_vector = [0] * hash_bits

#     for token in tokens:
#         token_hash = int(hashlib.md5(token.encode('utf-8')).hexdigest(), 16)

#         for i in range(hash_bits):
#             bit = (token_hash >> i) & 1
#             hash_vector[i] += 1 if bit == 1 else -1

#     simhash = 0
#     for i in range(hash_bits):
#         if hash_vector[i] > 0:
#             simhash |= (1 << i)

#     return simhash

# def hamming_distance(hash1, hash2):
#     xor_result = hash1 ^ hash2
#     return bin(xor_result).count('1')

# def find_similar_items(items, threshold=10, hash_bits=64):
#     simhashes = [(item, compute_simhash(item, hash_bits)) for item in items]
#     similar_pairs = []

#     for i in range(len(simhashes)):
#         for j in range(i + 1, len(simhashes)):
#             item1, hash1 = simhashes[i]
#             item2, hash2 = simhashes[j]
#             distance = hamming_distance(hash1, hash2)
#             if distance <= threshold:
#                 similar_pairs.append((item1, item2, distance))

#     if len(similar_pairs) != 0:
#         print("similar pairs found")
#         return True
#     return False

#     #return similar_pairs

# import hashlib

# def compute_simhash(text, hash_bits=64):
#     tokens = text.split()
#     hash_vector = [0] * hash_bits

#     for token in tokens:
#         token_hash = int(hashlib.md5(token.encode('utf-8')).hexdigest(), 16)

#         for i in range(hash_bits):
#             bit = (token_hash >> i) & 1
#             hash_vector[i] += 1 if bit == 1 else -1

#     simhash = 0
#     for i in range(hash_bits):
#         if hash_vector[i] > 0:
#             simhash |= (1 << i)

#     return simhash

# def hamming_distance(hash1, hash2):
#     xor_result = hash1 ^ hash2
#     return bin(xor_result).count('1')

# def find_similar_items(items, threshold=10, hash_bits=64):
#     simhashes = [(item, compute_simhash(item, hash_bits)) for item in items]
#     similar_pairs = []

#     for i in range(len(simhashes)):
#         for j in range(i + 1, len(simhashes)):
#             item1, hash1 = simhashes[i]
#             item2, hash2 = simhashes[j]
#             distance = hamming_distance(hash1, hash2)
#             if distance <= threshold:
#                 similar_pairs.append((item1, item2, distance))

#     if len(similar_pairs) != 0:
#         print("similar pairs found")
#         return True
#     return False

#     #return similar_pairs


# large_list = [
#     "The quick brown fox jumps over the lazy dog",
#     "The quick brown fox leapt over the lazy dog",
#     "An entirely different sentences",
#     "The quick fox jumps over a lazy dog"
# ]

# threshold = 10
# similar_items = find_similar_items(large_list, threshold=threshold)

# print("similar_items:", similar_items)


import hashlib

# Initialize a global list to store hashes
stored_hashes = []

def compute_simhash(items, hash_bits=64):
    """Compute the SimHash for a given list of items."""
    hash_vector = [0] * hash_bits

    for item in items:
        token_hash = int(hashlib.md5(item.encode('utf-8')).hexdigest(), 16)

        for i in range(hash_bits):
            bit = (token_hash >> i) & 1
            hash_vector[i] += 1 if bit == 1 else -1

    simhash = 0
    for i in range(hash_bits):
        if hash_vector[i] > 0:
            simhash |= (1 << i)

    return simhash

def hamming_distance(hash1, hash2):
    """Calculate the Hamming distance between two SimHashes."""
    xor_result = hash1 ^ hash2
    return bin(xor_result).count('1')

def is_duplicate(new_list, similarity=0.99999, hash_bits=64):
    """Check if the new list is similar to any stored lists based on similarity percentage."""
    global stored_hashes
    new_hash = compute_simhash(new_list, hash_bits)

    # Calculate the threshold for the Hamming distance
    max_allowed_differences = int(hash_bits * (1 - similarity))

    for stored_hash in stored_hashes:
        if hamming_distance(new_hash, stored_hash) <= max_allowed_differences:
            return True  # Similar pair found

    # If no similar pairs, store the new hash and return False
    stored_hashes.append(new_hash)
    return False
