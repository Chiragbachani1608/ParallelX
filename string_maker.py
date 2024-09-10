# Let's create a large sample text file named 'large_file.txt' for testing purposes.
# We'll generate random words to simulate a large text file.

import random
import string

# Function to generate a random word
def generate_word(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Create a large file with random words
file_path = '/mnt/data/large_file.txt'
with open(file_path, 'w') as file:
    for _ in range(100000):  # 100,000 words
        word = generate_word(random.randint(3, 8))  # Random word length between 3 to 8
        file.write(word + ' ')
        if random.random() < 0.1:  # Newline every now and then
            file.write('\n')

file_path
