import os


def main():
    admin_path = os.getenv("ADMIN_PATH", "not set")
    print("Hello from app.py!")
    print(f"ADMIN_PATH={admin_path}")
    print("Add your application logic to this file.")


if __name__ == "__main__":
    main()
