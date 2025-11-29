import asyncio
import time
from server import Server, start_server
from client import client

# List of clients with spawn delay, name, and capabilities
CLIENTS = [
    (2, "client-1", {3}),
    (2, "client-2", {1, 3}),
    (3, "client-3", {4}),
    (4, "client-4", {3}),
    (5, "client-5", {4, 5}),
    (6, "client-6", {2, 5}),
    (7, "client-7", {2}),
]


async def launch_client_after_delay(delay, name, caps):
    """
    Spawns a client after a given delay.
    All clients start asynchronously → fastest system possible.
    """
    await asyncio.sleep(delay)
    print(f"[MAIN] Launching {name}")
    await client(name, caps)


async def start_clients():
    """
    Starts all clients in parallel (each with its join delay).
    """
    tasks = []
    for delay, name, caps in CLIENTS:
        tasks.append(asyncio.create_task(
            launch_client_after_delay(delay, name, caps)
        ))

    await asyncio.gather(*tasks)


async def read_tasks_file(server: Server):
    """
    Reads the tasks.txt file and feeds tasks to the server.
    -1,-1 marks end of file.
    """
    with open("tasks.txt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            a, b = map(int, line.split(","))
            if a == -1 and b == -1:
                # Mark that no more tasks are coming
                server.end_file = True
                return

            await server.add_task(a, b)


async def main():
    """
    Entire system orchestrator:
    - starts server
    - starts task reader
    - starts all clients
    - computes makespan and runtime
    """
    wall_start = time.time()

    server = Server()

    # Start server + tasks file reader
    asyncio.create_task(start_server(server))
    asyncio.create_task(read_tasks_file(server))

    # Start all clients asynchronously
    await start_clients()

    # Compute performance metrics
    completed = [t for t in server.tasks.values() if t.end_time]
    start_times = [t.start_time for t in completed]
    end_times = [t.end_time for t in completed]

    wall_end = time.time()

    print("\n========== SYSTEM SUMMARY ==========")
    print(f"Total wall-clock runtime: {wall_end - wall_start:.3f} sec")

    if start_times and end_times:
        makespan = max(end_times) - min(start_times)
        print(f"Makespan                 : {makespan:.3f} sec")

    print("====================================\n")

    # Detailed task info
    for t in sorted(server.tasks.values(), key=lambda x: x.id):
        print(
            f"Task {t.id}: type={t.func_type}, assigned_to={t.assigned_to}, "
            f"state={t.state}, start={t.start_time}, end={t.end_time}"
        )


if __name__ == "__main__":
    asyncio.run(main())
