#!/usr/bin/env python3
"""
Helper script to encode credentials for the configuration file
Python 3.6
"""

import base64
import sys

def simple_encrypt(text, key="default_key"):
    """Simple base64 encoding for credentials"""
    try:
        combined = f"{key}:{text}"
        encoded = base64.b64encode(combined.encode()).decode()
        return encoded
    except Exception as e:
        print(f"Error encoding: {e}")
        return text

def simple_decrypt(encoded_text, key="default_key"):
    """Simple base64 decoding for verification"""
    try:
        decoded = base64.b64decode(encoded_text.encode()).decode()
        if decoded.startswith(f"{key}:"):
            return decoded[len(f"{key}:"):]
        return decoded
    except Exception as e:
        print(f"Error decoding: {e}")
        return encoded_text

def main():
    if len(sys.argv) != 3:
        print("Usage: python encode_credentials.py <username> <password>")
        print("Example: python encode_credentials.py myuser mypass")
        sys.exit(1)
    
    username = sys.argv[1]
    password = sys.argv[2]
    
    encoded_user = simple_encrypt(username)
    encoded_pass = simple_encrypt(password)
    
    print(f"Encoded username: {encoded_user}")
    print(f"Encoded password: {encoded_pass}")
    print()
    print("Add these to your config/config.cfg file:")
    print(f"user_salt = {encoded_user}")
    print(f"password_salt = {encoded_pass}")
    print()
    print("Verification:")
    print(f"Decoded username: {simple_decrypt(encoded_user)}")
    print(f"Decoded password: {simple_decrypt(encoded_pass)}")

if __name__ == "__main__":
    main()
