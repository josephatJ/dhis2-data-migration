import random
import string

def get_random_string(length):
    letters_numbers = [*string.ascii_lowercase,*string.ascii_letters, *string.digits]
    random_str = ''.join(random.choice(letters_numbers) for i in range(length))
    return random_str