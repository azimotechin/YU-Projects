import bcrypt
import datetime
import redis
import json # Use json for safe serialization instead of eval
import logging


class UserManager:
    def __init__(self, redis_client):
        self.r = redis_client
    
    def create_user(self, user):
        """Creates a user hash. Accounts are stored separately."""
        user_key = f"user:{user.email}"
        if self.r.exists(user_key):
            raise ValueError(f"User with email {user.email} already exists")

        user_data = {
            "name": user.name,
            "email": user.email,
            "created_at": datetime.datetime.now().isoformat()
        } 
        self.r.hset(user_key, mapping=user_data)
    
    def add_account_to_user(self, user_email, account_name):
        """Adds a new, unique account to a user's set of accounts."""
        user_accounts_key = f"user_accounts:{user_email}"
        
        # SADD returns 1 if the item was added, 0 if it already existed.
        # This is the most efficient way to check and add.
        if self.r.sadd(user_accounts_key, account_name):
            # The account was new and has been added.
            return True, f"Account '{account_name}' added successfully."
        else:
            # The account already existed in the set.
            return False, f"Account '{account_name}' already exists for this user."

    def get_user_accounts(self, user_email):
        """Retrieves all accounts for a user from their set."""
        user_accounts_key = f"user_accounts:{user_email}"
        # SMEMBERS returns all members of the set.
        accounts = self.r.smembers(user_accounts_key)
        # The Redis client has decode_responses=True, so 'accounts' is a set of strings.
        # No decoding is necessary.
        return sorted(accounts)

    def get_all_users(self):
        """Retrieves all users and their associated accounts."""
        users = []
        for user_key in self.r.scan_iter("user:*"):
            user_data = self.r.hgetall(user_key)
            user_email = user_data.get("email")
            if user_email:
                accounts = self.get_user_accounts(user_email)
                users.append({
                    "name": user_data.get("name"),
                    "email": user_email,
                    "created_at": user_data.get("created_at"),
                    "accounts": ", ".join(accounts) if accounts else "None"
                })
        return users

    def delete_user(self, user_email):
        """Deletes a user and all their accounts."""
        user_key = f"user:{user_email}"
        user_accounts_key = f"user_accounts:{user_email}"
        
        # Delete the user hash and their accounts set.
        self.r.delete(user_key)
        self.r.delete(user_accounts_key)
        
        return f"User {user_email} and their accounts have been deleted."