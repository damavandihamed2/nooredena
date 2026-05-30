import os
from pathlib import Path
from dotenv import load_dotenv, set_key


class EnvManager:

    def __init__(self, path: str | None = None, filename: str = ".env"):

        if path:
            self.env_path = f"{path}/{filename}"
        else:
            try:
                current_dir = Path(__file__).parent.resolve()
                self.env_path = str(current_dir / filename)
            except:
                self.env_path = f"{path}/{filename}"


        if not os.path.exists(self.env_path):
            with open(self.env_path, "w") as f:
                f.write("# Generated Environment File\n")

        load_dotenv(self.env_path)

    def get_token(self, key: str):
        return os.getenv(key)

    def update_or_add(self, key: str, value: str):
        set_key(self.env_path, key, value, quote_mode='always')
        os.environ[key] = value



if __name__ == "__main__":
    manager = EnvManager()

    manager.update_or_add("ACCESS_TOKEN", "initial_token_123")
    current_token = manager.get_token("ACCESS_TOKEN")
    print(f"Current Token: {current_token}")

    manager.update_or_add("ACCESS_TOKEN", "new_secure_token_456")
    updated_token = manager.get_token("ACCESS_TOKEN")
    print(f"Updated Token: {updated_token}")
