from pathlib import Path
import os

from dotenv import load_dotenv

from src.ledger.schema import initialize_database

def main() -> None:
    load_dotenv()
    db_path = Path(os.getenv("DATABASE_PATH", "data/bot.db"))
    print(f"Initializing database at: {db_path}")
    initialize_database(db_path)
    print("✓ Database initialized successfully")
    print(f"✓ Location: {db_path.absolute()}")

if __name__ == "__main__":
    main()
