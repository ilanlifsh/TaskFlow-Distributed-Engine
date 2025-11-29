import asyncio
import time
from server import Server, start_server
from client import client

# List of clients with spawn delay, name, and capabilities
# CLIENTS = [
#     (2, "client-1", {3, 5}),
#     (2, "client-2", {5}),
#     (3, "client-3", {2, 4}),
#     (4, "client-4", {3, 4}),
#     (5, "client-5", {2, 4, 5}),
#     (6, "client-6", {4}),
# ]
CLIENTS = [
   (2, "client-1", {3}),
   (2, "client-2", {1, 3}),
   (3, "client-3", {4}),
   (4, "client-4", {3}),
   (5, "client-5", {4, 5}),
   (6, "client-6", {2, 5}),
   (7, "client-7", {2}),
]
#CLIENTS = [
#    (2, "client-1", {3, 5}),
#    (2, "client-2", {1, 5}),
#    (3, "client-3", {2, 4}),
#    (4, "client-4", {3, 4}),
#    (5, "client-5", {3, 4}),
#    (6, "client-6", {2, 4, 5}),
#    (7, "client-7", {1, 4}),
#]


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
        tasks.append(
            asyncio.create_task(
                launch_client_after_delay(delay, name, caps)
            )
        )

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
                # Mark that no more tasks are coming (use server API)
                await server.mark_end()
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

    # Start TCP server in the background
    server_task = asyncio.create_task(start_server(server))

    # Start tasks file reader and clients in parallel
    reader_task = asyncio.create_task(read_tasks_file(server))
    clients_task = asyncio.create_task(start_clients())

    # Wait until reader finished and all clients finished
    await asyncio.gather(reader_task, clients_task)

    wall_end = time.time()

    # Stop the TCP server gracefully
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    # ================== PERFORMANCE METRICS ===================

    print("\n========== SYSTEM SUMMARY ==========")
    print(f"Total wall-clock runtime: {wall_end - wall_start:.3f} sec")

    # Makespan definition here:
    # from first task start/arrival to last task end (completed or discarded)
    all_tasks = list(server.tasks.values())

    # For tasks that never started (e.g. unsupported type), use arrival as "start"
    start_times = [
        (t.start_time if t.start_time is not None else t.arrival)
        for t in all_tasks
    ]

    # end_time is set for completed AND discarded tasks
    end_times = [
        t.end_time
        for t in all_tasks
        if t.end_time is not None
    ]

    if start_times and end_times:
        makespan = max(end_times) - min(start_times)
        print(f"Makespan                 : {makespan:.3f} sec")
    else:
        print("Makespan                 : N/A (no tasks)")

    print("====================================\n")

    # Detailed task info
    for t in sorted(server.tasks.values(), key=lambda x: x.id):
        print(
            f"Task {t.id}: type={t.func_type}, assigned_to={t.assigned_to}, "
            f"state={t.state}, start={t.start_time}, end={t.end_time}"
        )


if __name__ == "__main__":
    asyncio.run(main())
