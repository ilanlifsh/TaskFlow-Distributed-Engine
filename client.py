import asyncio

from protocol import (
    encode_register,
    encode_result,
    parse_message,
    is_task,
    is_no_tasks
)

# Same time mapping as server
TASK_EXEC_TIMES = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}

# ------------------------------------------------------------
# FUNCTIONS BY TYPE
# ------------------------------------------------------------

def func_type_1(param): return param * param
def func_type_2(param): return param * param * param
def func_type_3(param): return param * 10

def func_type_4(param):
    if param <= 1:
        return param
    a, b = 0, 1
    for _ in range(param):
        a, b = b, a + b
    return a

def func_type_5(param): return sum(range(param + 1))

FUNCTION_MAP = {
    1: func_type_1,
    2: func_type_2,
    3: func_type_3,
    4: func_type_4,
    5: func_type_5,
}

# ------------------------------------------------------------
# RUN SINGLE TASK
# ------------------------------------------------------------

async def run_task(writer, tid, func, param, name):

    compute_func = FUNCTION_MAP.get(func)
    result = compute_func(param) if compute_func else "ERR"

    await asyncio.sleep(TASK_EXEC_TIMES[func])

    msg = encode_result(tid, name, func, result)

    try:
        writer.write(msg.encode())
        await writer.drain()
    except Exception:
        print(f"[{name}] ERROR sending RESULT for task {tid}")

# ------------------------------------------------------------
# CLIENT LOOP
# ------------------------------------------------------------

async def client(name, caps):

    reader, writer = await asyncio.open_connection("127.0.0.1", 5000)

    # Register using protocol encoder
    writer.write(encode_register(name, caps).encode())
    await writer.drain()

    print(f"[{name}] connected with caps={caps}")
    tasks_running = []

    try:
        while True:
            line = await reader.readline()
            if not line:
                break

            parts = parse_message(line.decode())

            if is_no_tasks(parts):
                print(f"[{name}] NO-TASKS received.")
                await asyncio.gather(*tasks_running, return_exceptions=True)
                writer.close()
                return

            if is_task(parts):
                tid = int(parts[1])
                func = int(parts[2])
                param = int(parts[3])

                t = asyncio.create_task(
                    run_task(writer, tid, func, param, name)
                )
                tasks_running.append(t)

    except ConnectionResetError:
        print(f"[{name}] server disconnected")

    writer.close()
