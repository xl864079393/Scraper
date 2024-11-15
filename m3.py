def remove_numbers(tokens):
    for token in tokens[:]:
        if token.isdigit():
            tokens.remove(token)
    return tokens

# Example usage
tokens = ["hello", "123", "1", "456"]
filtered_tokens = remove_numbers(tokens)
print(filtered_tokens)  # Output: ['hello', 'world']