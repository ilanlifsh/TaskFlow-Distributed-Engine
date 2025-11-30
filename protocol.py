"""
protocol.py
Defines the message format shared by server and clients.
"""

REGISTER = "REGISTER"
TASK = "TASK"
RESULT = "RESULT"
NO_TASKS = "NO-TASKS"


# ------------------------------------------------------------
# ENCODERS
# ------------------------------------------------------------

def encode_register(name: str, caps) -> str:
    caps_str = " ".join(str(c) for c in caps)
    return f"{REGISTER} {name} {caps_str}\n"


def encode_task(task_id: int, func_type: int, param: int) -> str:
    return f"{TASK} {task_id} {func_type} {param}\n"


def encode_result(task_id: int, client_name: str, func: int, value: int) -> str:
    return f"{RESULT} {task_id} {client_name} {func} {value}\n"


def encode_no_tasks() -> str:
    return f"{NO_TASKS}\n"


# ------------------------------------------------------------
# DECODER
# ------------------------------------------------------------

def parse_message(line: str):
    return line.strip().split()


# ------------------------------------------------------------
# VALIDATION
# ------------------------------------------------------------

def is_register(parts): return len(parts) >= 3 and parts[0] == REGISTER
def is_task(parts): return len(parts) == 4 and parts[0] == TASK
def is_result(parts): return len(parts) == 5 and parts[0] == RESULT
def is_no_tasks(parts): return len(parts) == 1 and parts[0] == NO_TASKS


# optional safe int
def safe_int(v):
    try: return int(v)
    except: return None
