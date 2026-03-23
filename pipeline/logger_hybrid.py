import os
import fcntl
import time
import datetime
import pathlib


LOCKS_DIR = pathlib.Path(__file__).parent.absolute() / "locks"


class HybridLogger:
    def __init__(self, filename):
        self.filename = filename
        self.permanent_offset = 0

        mode = "a" if os.path.exists(filename) else "w"
        with open(self.filename, mode) as f:
            if mode == "w":
                f.write(
                    f"=== Experiment Log Started at {datetime.datetime.now()} ===\n"
                )
            else:
                f.write(f"\n=== Runner Restarted at {datetime.datetime.now()} ===\n")
        self.permanent_offset = os.path.getsize(self.filename)

    def log(self, message):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}\n"

        try:
            lock_path = f"{self.filename}.lock"
            got_lock = False
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                got_lock = True
            except FileExistsError:
                try:
                    age = time.time() - os.path.getmtime(lock_path)
                    if age > 5:
                        os.unlink(lock_path)
                        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                        os.close(fd)
                        got_lock = True
                    else:
                        got_lock = False
                except Exception:
                    got_lock = False
            except Exception:
                got_lock = False

            try:
                with open(self.filename, "a") as f:
                    f.write(line)
            finally:
                if got_lock:
                    try:
                        os.unlink(lock_path)
                    except Exception:
                        pass
        except Exception:
            pass
