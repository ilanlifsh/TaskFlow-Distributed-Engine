import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional, Set, Deque
from collections import deque

# Maximum waiting time for a task before being discarded (in seconds)
TASK_MAX_WAIT = 60

# Execution time per function type (for information only)
TASK_EXEC_TIMES = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}


# ============================================================
# TASK AND CLIENT DATA STRUCTURES
# ============================================================

@dataclass
class Task:
    """
    Represents a single task in the system.
    Lifecycle: pending → running → completed / discarded.
    """
    id: int
    func_type: int               # Task type (determines which clients can run it)
    param: int                   # Input parameter

    arrival: float               # When task entered the system
    deadline: float              # Absolute time after which task is discarded

    assigned_to: Optional[str] = None
    state: str = "pending"       # "pending" / "running" / "completed" / "discarded"

    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class ClientInfo:
    """
    Holds information for a connected client.
    """
    name: str
    caps: Set[int]                 # Function types the client can execute
    running: Set[int]              # IDs of tasks currently executing
    writer: asyncio.StreamWriter   # TCP writer to send tasks to client
    capacity: int                  # Max parallel tasks = number of capabilities
    alive: bool = True             # Used to detect disconnects


# ============================================================
# SERVER IMPLEMENTATION
# ============================================================

class Server:
    """
    Main server class responsible for:
    - receiving tasks from file
    - registering clients
    - assigning tasks
    - receiving results
    - reassigning tasks if a client disconnects
    - discarding tasks after 60s if no suitable client executes them
    """

    def __init__(self):
        # Queue of pending tasks per function type
        self.tasks_by_type: Dict[int, Deque[Task]] = {
            t: deque() for t in TASK_EXEC_TIMES
        }

        # All tasks (by id)
        self.tasks: Dict[int, Task] = {}

        # All connected clients
        self.clients: Dict[str, ClientInfo] = {}

        self.end_file = False              # True when no more tasks will be added
        self._next_id = 1
        self._cond = asyncio.Condition()

    # ----------------------------------------------------------
    async def add_task(self, func: int, param: int):
        """
        Adds a new incoming task to the server.
        """
        async with self._cond:
            now = time.time()
            t = Task(
                id=self._next_id,
                func_type=func,
                param=param,
                arrival=now,
                deadline=now + TASK_MAX_WAIT,
            )
            self._next_id += 1

            self.tasks[t.id] = t
            self.tasks_by_type[func].append(t)

            # Wake up senders that might be waiting for work
            self._cond.notify_all()

    # ----------------------------------------------------------
    async def mark_end(self):
        """
        Marks that no more tasks will arrive from the file.
        Tasks that cannot be executed will still remain pending until
        their 60-second deadline expires.
        """
        async with self._cond:
            self.end_file = True
            self._cond.notify_all()

    # ----------------------------------------------------------
    async def register(self, name: str, caps: Set[int], writer: asyncio.StreamWriter):
        """
        Registers a new client and stores its capabilities and writer.
        """
        async with self._cond:
            info = ClientInfo(
                name=name,
                caps=caps,
                running=set(),
                writer=writer,
                capacity=len(caps),   # Rule: number of types = parallel tasks
            )
            self.clients[name] = info
            print(f"[SERVER] {name} registered with caps={caps}")
            self._cond.notify_all()

    # ----------------------------------------------------------
    def cleanup_expired(self, func: int):
        """
        Removes tasks from a function queue if their deadline has passed.
        Also sets end_time so makespan includes discarded tasks.
        """
        q = self.tasks_by_type[func]
        now = time.time()

        while q and q[0].deadline < now:
            t = q.popleft()
            if t.state == "pending":
                t.state = "discarded"
                t.end_time = now       # mark when it was dropped
                print(f"[SERVER] Task {t.id} of type {t.func_type} discarded (deadline exceeded).")

    def cleanup_all_expired(self):
        """
        Cleans expired tasks for all function types.
        Even unsupported-task types get cleaned here.
        """
        for func in self.tasks_by_type.keys():
            self.cleanup_expired(func)

    # ----------------------------------------------------------
    def _all_tasks_done(self) -> bool:
        """
        Returns True if every task is either completed or discarded.
        """
        if not self.tasks:
            return False
        return all(t.state in ("completed", "discarded") for t in self.tasks.values())

    # ----------------------------------------------------------
    async def select_task(self, client: ClientInfo) -> Optional[Task]:
        """
        Chooses the best next task for a client:
        - must match client's capabilities
        - uses shortest-queue priority
        - respects client's parallel capacity
        - enforces: a client cannot run two tasks of the same func_type at once
        - returns None only when all tasks are done/discarded (AND file ended)
        """
        while True:
            async with self._cond:
                # If client disconnected, stop assigning
                if not client.alive:
                    return None

                # If client is already full (parallel limit), wait
                if len(client.running) >= client.capacity:
                    await self._cond.wait()
                    continue

                best_task = None
                best_len = 999999

                # Try to pick best task among types this client can execute
                for f in client.caps:
                    # NEW RULE: skip this type if the client already runs a task of this type
                    if any(self.tasks[tid].func_type == f for tid in client.running):
                        continue

                    q = self.tasks_by_type[f]
                    if q and len(q) < best_len:
                        best_len = len(q)
                        best_task = q[0]

                # Found a suitable task → assign it
                if best_task:
                    self.tasks_by_type[best_task.func_type].popleft()
                    best_task.assigned_to = client.name
                    best_task.start_time = time.time()
                    best_task.state = "running"
                    client.running.add(best_task.id)
                    return best_task

                # No task currently assignable to this client.
                # If file ended AND all tasks are completed/discarded → done.
                if self.end_file and self._all_tasks_done():
                    return None

                # Otherwise, wait until something changes
                await self._cond.wait()

    # ----------------------------------------------------------
    async def report(self, client_name: str, tid: int, func: int, result: int):
        """
        Handles RESULT messages from clients.
        """
        async with self._cond:
            t = self.tasks[tid]
            c = self.clients[client_name]

            t.state = "completed"
            t.end_time = time.time()

            if tid in c.running:
                c.running.remove(tid)

            print(f"[SERVER] Task {tid} completed by {client_name}, func={func}, result={result}")

            self._cond.notify_all()

    # ----------------------------------------------------------
    async def client_task_sender(self, client: ClientInfo):
        """
        Continuously tries to send new tasks to the client
        whenever it has free parallel execution slots.
        """
        while client.alive:
            task = await self.select_task(client)

            # None means: all tasks are completed or discarded.
            if task is None:
                client.writer.write(b"NO-TASKS\n")
                await client.writer.drain()

                # Wait for all running tasks to finish reporting
                while client.running:
                    await asyncio.sleep(0.1)

                client.alive = False
                return

            # Send task
            msg = f"TASK {task.id} {task.func_type} {task.param}\n"
            client.writer.write(msg.encode())
            await client.writer.drain()

    # ----------------------------------------------------------
    async def client_task_receiver(self, client: ClientInfo, reader: asyncio.StreamReader):
        """
        Receives RESULT messages from the client asynchronously.
        """
        while client.alive:
            line = await reader.readline()
            if not line:   # client disconnected
                client.alive = False
                return

            parts = line.decode().strip().split()
            if parts and parts[0] == "RESULT":
                tid = int(parts[1])
                name = parts[2]
                func = int(parts[3])
                result = int(parts[4])
                await self.report(name, tid, func, result)

    # ----------------------------------------------------------
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handles a new client connection and starts its sender/receiver loops.
        """
        line = await reader.readline()
        parts = line.decode().strip().split()

        if not parts or parts[0] != "REGISTER":
            writer.close()
            return

        name = parts[1]
        caps = set(map(int, parts[2:]))

        await self.register(name, caps, writer)
        client = self.clients[name]

        sender = asyncio.create_task(self.client_task_sender(client))
        receiver = asyncio.create_task(self.client_task_receiver(client, reader))

        await asyncio.wait([sender, receiver], return_when=asyncio.FIRST_COMPLETED)

        print(f"[SERVER] Client {name} disconnected.")
        client.alive = False
        await self.reassign_running_tasks(client)
        writer.close()

    # ----------------------------------------------------------
    async def reassign_running_tasks(self, client: ClientInfo):
        """
        Returns running tasks of a dead client back to pending queues.
        Resets their deadline to provide a fresh 60-second window.
        """
        async with self._cond:
            now = time.time()

            for tid in list(client.running):
                t = self.tasks[tid]
                t.state = "pending"
                t.assigned_to = None
                t.start_time = None
                t.end_time = None
                t.deadline = now + TASK_MAX_WAIT   # new 60s window

                self.tasks_by_type[t.func_type].appendleft(t)

            client.running.clear()
            self._cond.notify_all()

    # ----------------------------------------------------------
    async def expiration_loop(self):
        """
        Background loop that periodically expires tasks whose 60s deadline passed.
        It also wakes up clients so they can notice that tasks were dropped.
        """
        while True:
            async with self._cond:
                self.cleanup_all_expired()
                self._cond.notify_all()

                # Optional: stop when file ended and everything is done
                if self.end_file and self._all_tasks_done():
                    return

            await asyncio.sleep(1.0)


# ============================================================
# MAKESPAN HELPER
# ============================================================

def compute_makespan(server: "Server") -> float:
    """
    Computes makespan = time from first task start/arrival
    to last task end (completed or discarded).
    """
    if not server.tasks:
        return 0.0

    tasks = list(server.tasks.values())

    # start: if started use start_time, else arrival
    start_times = [
        (t.start_time if t.start_time is not None else t.arrival)
        for t in tasks
    ]

    # end: set for both completed AND discarded
    end_times = [
        t.end_time for t in tasks
        if t.end_time is not None
    ]

    if not start_times or not end_times:
        return 0.0

    return max(end_times) - min(start_times)


# ============================================================
# START TCP SERVER
# ============================================================

async def start_server(server: Server):
    """
    Creates a TCP server and listens forever.
    Also starts the background expiration loop.
    """
    print("[SERVER] Running on 127.0.0.1:5000...")

    # Start the expiration loop in the background
    asyncio.create_task(server.expiration_loop())

    srv = await asyncio.start_server(
        lambda r, w: server.handle_client(r, w),
        "127.0.0.1",
        5000
    )

    async with srv:
        await srv.serve_forever()
