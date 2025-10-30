import bcrypt
import redis
import json
import os
from typing import Optional, List, Dict

class UserManager:
    def __init__(self, redis_client=None):
        if redis_client is None:
            redis_host = os.getenv('REDIS_HOST', 'redis-primary')
            self.redis_client = redis.Redis(
                host=redis_host, 
                port=6379, 
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )
        else:
            self.redis_client = redis_client
    
    def hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against its hash"""
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    
    def create_user(self, email: str, password: str, full_name: str = "") -> bool:
        """Create a new user account"""
        try:
            if self.redis_client.hexists("users", email):
                return False
            
            hashed_password = self.hash_password(password)
            
            user_data = {
                "email": email,
                "password_hash": hashed_password,
                "full_name": full_name,
                "created_at": "2025-01-01T00:00:00", 
                "accounts": "[]"
            }
            
            self.redis_client.hset("users", email, json.dumps(user_data))
            return True
            
        except Exception as e:
            print(f"Error creating user: {e}")
            return False
    
    def authenticate_user(self, email: str, password: str) -> bool:
        """Authenticate a user"""
        try:
            user_data_str = self.redis_client.hget("users", email)
            if not user_data_str:
                return False
            
            user_data = json.loads(user_data_str)
            return self.verify_password(password, user_data["password_hash"])
            
        except Exception as e:
            print(f"Error authenticating user: {e}")
            return False
    
    def get_user(self, email: str) -> Optional[Dict]:
        """Get user data (without password hash)"""
        try:
            user_data_str = self.redis_client.hget("users", email)
            if not user_data_str:
                return None
            
            user_data = json.loads(user_data_str)
            user_data.pop("password_hash", None)
            return user_data
            
        except Exception as e:
            print(f"Error getting user: {e}")
            return None
    
    def create_trading_account(self, user_email: str, account_name: str, initial_balance: float = 0.0) -> bool:
        """Create a new trading account for a user"""
        try:
            user_data_str = self.redis_client.hget("users", user_email)
            if not user_data_str:
                return False
            
            user_data = json.loads(user_data_str)
            accounts = json.loads(user_data.get("accounts", "[]"))
            
            for account in accounts:
                if account["name"] == account_name:
                    return False
            
            new_account = {
                "name": account_name,
                "balance": initial_balance,
                "created_at": "2025-01-01T00:00:00"
            }
            
            accounts.append(new_account)
            user_data["accounts"] = json.dumps(accounts)
            
            self.redis_client.hset("users", user_email, json.dumps(user_data))
            return True
            
        except Exception as e:
            print(f"Error creating trading account: {e}")
            return False
    
    def get_user_accounts(self, user_email: str) -> List[Dict]:
        """Get all trading accounts for a user"""
        try:
            user_data_str = self.redis_client.hget("users", user_email)
            if not user_data_str:
                return []
            
            user_data = json.loads(user_data_str)
            accounts = json.loads(user_data.get("accounts", "[]"))
            return accounts
            
        except Exception as e:
            print(f"Error getting user accounts: {e}")
            return []
    
    def update_account_balance(self, user_email: str, account_name: str, new_balance: float) -> bool:
        """Update the balance of a trading account"""
        try:
            user_data_str = self.redis_client.hget("users", user_email)
            if not user_data_str:
                return False
            
            user_data = json.loads(user_data_str)
            accounts = json.loads(user_data.get("accounts", "[]"))
            
            for account in accounts:
                if account["name"] == account_name:
                    account["balance"] = new_balance
                    break
            else:
                return False
            
            user_data["accounts"] = json.dumps(accounts)
            self.redis_client.hset("users", user_email, json.dumps(user_data))
            return True
            
        except Exception as e:
            print(f"Error updating account balance: {e}")
            return False
    
    def delete_account(self, user_email: str, account_name: str) -> bool:
        """Delete a trading account"""
        try:
            user_data_str = self.redis_client.hget("users", user_email)
            if not user_data_str:
                return False
            
            user_data = json.loads(user_data_str)
            accounts = json.loads(user_data.get("accounts", "[]"))
            
            accounts = [acc for acc in accounts if acc["name"] != account_name]
            
            user_data["accounts"] = json.dumps(accounts)
            self.redis_client.hset("users", user_email, json.dumps(user_data))
            return True
            
        except Exception as e:
            print(f"Error deleting account: {e}")
            return False
    
    def list_all_users(self) -> List[Dict]:
        """List all users (admin function)"""
        try:
            users = []
            all_users = self.redis_client.hgetall("users")
            
            for email, user_data_str in all_users.items():
                user_data = json.loads(user_data_str)
                user_data.pop("password_hash", None)
                users.append(user_data)
            
            return users
            
        except Exception as e:
            print(f"Error listing users: {e}")
            return []
    
    def delete_user(self, email: str) -> bool:
        """Delete a user account (admin function)"""
        try:
            return bool(self.redis_client.hdel("users", email))
        except Exception as e:
            print(f"Error deleting user: {e}")
            return False
