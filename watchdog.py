"""Watchdog — keeps bot.py alive, restarts on crash with backoff."""
import subprocess, time, sys, os, datetime

BOT_SCRIPT  = os.path.join(os.path.dirname(__file__), "bot.py")
LOCK_FILE   = os.path.join(os.path.dirname(__file__), "bot.lock")
LOG_FILE    = os.path.join(os.path.dirname(__file__), "bot_stderr.log")
STDOUT_FILE = os.path.join(os.path.dirname(__file__), "bot_stdout.log")
PYTHON      = sys.executable

MIN_UPTIME_SEC  = 30   # crashes faster than this → increase backoff
MAX_BACKOFF_SEC = 300  # cap at 5 min
backoff         = 5

def ts():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def bot_already_running() -> bool:
    if not os.path.exists(LOCK_FILE):
        return False
    try:
        pid = int(open(LOCK_FILE).read().strip())
        # send signal 0 — just checks if process exists
        os.kill(pid, 0)
        return True
    except (ValueError, OSError):
        return False

def run():
    global backoff
    if bot_already_running():
        print(f"[{ts()}] Bot already running per lock file — watchdog standing by")

    while True:
        if bot_already_running():
            time.sleep(10)
            continue

        print(f"[{ts()}] Starting bot.py (backoff={backoff}s) ...")
        start = time.time()

        with open(STDOUT_FILE, "a") as out, open(LOG_FILE, "a") as err:
            proc = subprocess.Popen(
                [PYTHON, BOT_SCRIPT],
                stdout=out,
                stderr=err,
                cwd=os.path.dirname(__file__),
            )

        proc.wait()
        uptime = time.time() - start
        code   = proc.returncode

        print(f"[{ts()}] Bot exited (code={code}, uptime={uptime:.0f}s)")

        if uptime < MIN_UPTIME_SEC:
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)
            print(f"[{ts()}] Fast crash — backing off {backoff}s")
        else:
            backoff = 5  # reset on healthy run

        print(f"[{ts()}] Restarting in {backoff}s ...")
        time.sleep(backoff)

if __name__ == "__main__":
    print(f"[{ts()}] Watchdog started (PID {os.getpid()})")
    try:
        run()
    except KeyboardInterrupt:
        print(f"[{ts()}] Watchdog stopped by user")
