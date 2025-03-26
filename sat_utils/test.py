from cryptography.fernet import Fernet

def get_claves(): 
    # Genera las claves
    print("FERNET_KEY:", Fernet.generate_key().decode())
    print("SECRET_KEY:", Fernet.generate_key().decode())

if __name__ == "__main__":
    get_claves()