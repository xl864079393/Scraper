# import hashlib

# # Initialize a global list to store hashes
# stored_hashes = []

# def compute_simhash(items, hash_bits=64):
#     """Compute the SimHash for a given list of items."""
#     hash_vector = [0] * hash_bits

#     for item in items:
#         token_hash = int(hashlib.md5(item.encode('utf-8')).hexdigest(), 16)
        
#         for i in range(hash_bits):
#             bit = (token_hash >> i) & 1
#             hash_vector[i] += 1 if bit == 1 else -1

#     simhash = 0
#     for i in range(hash_bits):
#         if hash_vector[i] > 0:
#             simhash |= (1 << i)

#     return simhash

# def hamming_distance(hash1, hash2):
#     """Calculate the Hamming distance between two SimHashes."""
#     xor_result = hash1 ^ hash2
#     return bin(xor_result).count('1')

# def is_duplicate(new_list, similarity=0.95, hash_bits=1280):
#     """Check if the new list is similar to any stored lists based on similarity percentage."""
#     global stored_hashes
#     new_hash = compute_simhash(new_list, hash_bits)
    
#     # Calculate the threshold for the Hamming distance
#     max_allowed_differences = int(hash_bits * (1 - similarity))
    
#     for stored_hash in stored_hashes:
#         if hamming_distance(new_hash, stored_hash) <= max_allowed_differences:
#             return True  # Similar pair found
    
#     # If no similar pairs, store the new hash and return False
#     stored_hashes.append(new_hash)
#     return False

from simhash import Simhash

# Global list to store SimHashes of stored lists
stored_hashes = []

def compute_list_simhash(items):
    return Simhash(items)

def hamming_distance(hash1, hash2):
    return hash1.distance(hash2)

def is_duplicate_list(new_list):
    global stored_hashes
    new_hash = compute_list_simhash(new_list)
    
    for stored_hash in stored_hashes:
        if hamming_distance(new_hash, stored_hash) <= 2 :
            return True  # Duplicate found

    # If no duplicate is found, store the new hash
    stored_hashes.append(new_hash)
    return False


