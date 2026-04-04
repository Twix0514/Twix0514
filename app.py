import argparse
import os
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Simple app entrypoint for Twix0514 repository.")
    parser.add_argument("--admin-path", dest="admin_path", help="Override ADMIN_PATH environment variable")
    parser.add_argument("--status", action="store_true", help="Show status information and exit")
    parser.add_argument("--greet", default="World", help="Greeting name")
    return parser.parse_args()


def main():
    args = parse_args()
    admin_path = args.admin_path or os.getenv("ADMIN_PATH")

    print("Hello from app.py!")
    print(f"Greet: {args.greet}")

    if admin_path:
        admin_path = str(Path(admin_path).expanduser())
        admin_path_obj = Path(admin_path)
        print(f"ADMIN_PATH={admin_path}")
        print(f"Resolved ADMIN_PATH: {admin_path_obj}")
        print("Exists:" if admin_path_obj.exists() else "Does not exist")
        if admin_path_obj.exists() and admin_path_obj.is_dir():
            print(f"Directory contents: {len(list(admin_path_obj.iterdir()))} entries")
    else:
        print("ADMIN_PATH is not set. Use --admin-path or set the ADMIN_PATH environment variable.")

    print(f"Working directory: {Path.cwd()}")
    if args.status:
        print("Status: ready")
        return 0

    print("\nAdd your application logic below this line.")
    print("- Use the admin path for configuration or filesystem tasks")
    print("- Replace this placeholder with the real behavior you need")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
