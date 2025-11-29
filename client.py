import asyncio

# Same time mapping as server
TASK_EXEC_TIMES = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}


# ============================================================
# FUNCTIONS PER TASK TYPE
# ============================================================

def func_type_1(param):
    return param * param              # square

def func_type_2(param):
    return param * param * param      # cube

def func_type_3(param):
    return param * 10                 # multiply by 10

def func_type_4(param):
    # fibonacci
    if param <= 1:
        return param
    a, b = 0, 1
    for _ in range(param):
        a, b = b, a + b
    return a

def func_type_5(param):
    # sum(0..param)
    return sum(range(param + 1))


# Mapping func_type → execution function
FUNCTION_MAP = {
    1: func_type_1,
    2: func_type_2,
    3: func_type_3,
    4: func_type_4,
    5: func_type_5,
}


# ============================================================
# RUN A SINGLE TASK
# ============================================================

async def run_task(writer, tid, func, param, name):
    """
    Executes a task locally based on func_type and sends RESULT back.
    Each task runs in its own coroutine → supports parallel work.
    """

    # Get correct function for this func_type
    compute_func = FUNCTION_MAP.get(func)

    # If type unknown, return error-like result
    if compute_func is None:
        result = "ERR"
    else:
        result = compute_func(param)

    # Simulate processing time based on task type
    await asyncio.sleep(TASK_EXEC_TIMES[func])

    msg = f"RESULT {tid} {name} {func} {result}\n"

    # Protect against connection closure
    try:
        writer.write(msg.encode())
        await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        print(f"[{name}] connection closed while sending RESULT for task {tid}.")
    except Exception as e:
        print(f"[{name}] unexpected error sending RESULT for task {tid}: {e}")


# ============================================================
# CLIENT MAIN LOOP
# ============================================================

async def client(name, caps):
    """
    Main client logic:
    - connect to server
    - register with capabilities
    - receive tasks
    - run tasks concurrently
    - gracefully close when NO-TASKS is received
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", 5000)

    # Send registration message
    reg = "REGISTER " + name + " " + " ".join(str(x) for x in caps) + "\n"
    writer.write(reg.encode())
    await writer.drain()

    print(f"[{name}] connected to server with caps={caps}")

    # Track all running task coroutines
    tasks_running = []

    try:
        while True:
            line = await reader.readline()
            if not line:
                break  # server closed connection

            parts = line.decode().strip().split()

            if parts[0] == "NO-TASKS":
                print(f"[{name}] No more tasks. Waiting for remaining tasks...")

                # Wait for all running tasks to finish
                await asyncio.gather(*tasks_running, return_exceptions=True)

                print(f"[{name}] All tasks finished. Disconnecting.")
                writer.close()
                return

            if parts[0] == "TASK":
                # Parse task
                tid = int(parts[1])
                func = int(parts[2])
                param = int(parts[3])

                # Run task in separate coroutine
                task = asyncio.create_task(
                    run_task(writer, tid, func, param, name)
                )
                tasks_running.append(task)

    except ConnectionResetError:
        print(f"[{name}] Server closed the connection.")

    writer.close()
