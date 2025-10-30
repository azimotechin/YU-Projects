import secrets
import string
import uuid

guid = uuid.uuid4()
print(f"this is the unique GUID: {guid}")

def generate_random_complex_key(length=8):
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(characters) for i in range(length))
#example usage:
random_complex_key = generate_random_complex_key()
print(f"this is the random complex key: {random_complex_key}") 