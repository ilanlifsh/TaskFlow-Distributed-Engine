import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional, Set, Deque
from collections import deque

# Maximum waiting time for a task before being discarded
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
    Lifecycle: pending → running → completed/discarded.
    """

    id: int                       # Unique task ID
    func_type: int                # Task type (determines which clients can run it)
    param: int                    # Input parameter for the computation

    arrival: float                # Timestamp when task entered the server
    deadline: float               # Max allowed waiting time before discard

    assigned_to: Optional[str] = None  # Client currently running the task
    state: str = "pending"             # Current status of the task - pending/running/completed/discarded

    start_time: Optional[float] = None # When execution started (set on TASK send)
    end_time: Optional[float] = None   # When client finished (set on RESULT)


@dataclass
class ClientInfo:
    """
    Holds information for a connected client.
    """
    name: str
    caps: Set[int]                 # function types the client can execute
    running: Set[int]              # ids of tasks currently being executed
    writer: asyncio.StreamWriter   # TCP writer to send tasks to client
    capacity: int                  # max parallel tasks = number of capabilities
    alive: bool = True             # used to detect disconnects


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
    """

    def __init__(self):
        # A separate queue for each function type
        self.tasks_by_type: Dict[int, Deque[Task]] = {
            t: deque() for t in TASK_EXEC_TIMES
        }

        # All tasks in the system
        self.tasks: Dict[int, Task] = {}

        # All connected clients
        self.clients: Dict[str, ClientInfo] = {}

        self.end_file = False   # becomes True when tasks file ends
        self._next_id = 1       # incremental task ID
        self._cond = asyncio.Condition()   # synchronizes between sender/receiver

    # ----------------------------------------------------------
    async def add_task(self, func, param):
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

            # Wake up any sender loops waiting for a task
            self._cond.notify_all()

    # ----------------------------------------------------------
    async def register(self, name, caps, writer):
        """
        Registers a new client and stores its capabilities and writer.
        """
        async with self._cond:
            info = ClientInfo(
                name=name,
                caps=caps,
                running=set(),
                writer=writer,
                capacity=len(caps)     # Rule: number of types = parallel tasks
            )
            self.clients[name] = info

            print(f"[SERVER] {name} registered with caps={caps}")

            self._cond.notify_all()

    # ----------------------------------------------------------
    def cleanup_expired(self, func):
        """
        Removes tasks from a function queue if their deadline has passed.
        """
        q = self.tasks_by_type[func]
        now = time.time()

        while q and q[0].deadline < now:
            t = q.popleft()
            if t.state == "pending":
                t.state = "discarded"

    # ----------------------------------------------------------
    async def select_task(self, client: ClientInfo):
        """
        Chooses the best next task for a client:
        - must match client's capabilities
        - must keep queue lengths balanced (shortest queue first)
        - respects client's parallel capacity
        """

        while True:
            async with self._cond:

                # If client disconnected, stop assigning
                if not client.alive:
                    return None

                # Client is already full (parallel limit)
                if len(client.running) >= client.capacity:
                    await self._cond.wait()
                    continue

                best_task = None
                best_len = 999999

                # Search all queues the client can execute
                for f in client.caps:
                    self.cleanup_expired(f)
                    q = self.tasks_by_type[f]

                    if q and len(q) < best_len:
                        best_len = len(q)
                        best_task = q[0]

                # Found a task → assign it
                if best_task:
                    self.tasks_by_type[best_task.func_type].popleft()
                    best_task.assigned_to = client.name
                    best_task.start_time = time.time()
                    best_task.state = "running"
                    client.running.add(best_task.id)
                    return best_task

                # If no tasks remain for this client
                if self.end_file:
                    if all(not self.tasks_by_type[f] for f in client.caps):
                        return None

                # Otherwise wait for new tasks
                await self._cond.wait()

    # ----------------------------------------------------------
    async def report(self, client_name, tid, func, result):
        """
        Handles RESULT messages from clients.
        """
        async with self._cond:
            t = self.tasks[tid]
            c = self.clients[client_name]

            t.state = "completed"
            t.end_time = time.time()

            # Free parallel slot
            if tid in c.running:
                c.running.remove(tid)

            print(f"[SERVER] Task {tid} completed by {client_name}, func={func}, result={result}")

            # Notify senders that this client has free capacity again
            self._cond.notify_all()

    # ----------------------------------------------------------
    async def client_task_sender(self, client: ClientInfo):
        """
        Continuously tries to send new tasks to the client
        whenever it has free parallel execution slots.
        """
        while client.alive:
            task = await self.select_task(client)

            # No more tasks → send NO-TASKS and wait until all client tasks finish
            if task is None:
                client.writer.write(b"NO-TASKS\n")
                await client.writer.drain()

                # Wait for all running tasks to finish before closing connection
                while client.running:
                    await asyncio.sleep(0.1)

                client.alive = False
                return

            # Send task
            msg = f"TASK {task.id} {task.func_type} {task.param}\n"
            client.writer.write(msg.encode())
            await client.writer.drain()

    # ----------------------------------------------------------
    async def client_task_receiver(self, client: ClientInfo, reader):
        """
        Receives RESULTS from the client asynchronously.
        """
        while client.alive:
            line = await reader.readline()
            if not line:   # client disconnected
                client.alive = False
                return

            parts = line.decode().strip().split()

            if parts[0] == "RESULT":
                tid = int(parts[1])
                name = parts[2]
                func = int(parts[3])
                result = int(parts[4])

                await self.report(name, tid, func, result)

    # ----------------------------------------------------------
    async def handle_client(self, reader, writer):
        """
        Handles a new client connection.

        Each client creates TWO independent async loops:
        - client_task_sender: sends tasks whenever the client has free capacity
        - client_task_receiver: receives RESULT messages from the client

        Both loops run in parallel so the client can work on multiple tasks at once.
        """

        # Read initial registration message: "REGISTER <name> <cap1> <cap2> ..."
        line = await reader.readline()
        parts = line.decode().strip().split()

        if parts[0] != "REGISTER":
            # Invalid first message → close connection
            writer.close()
            return

        # Client's unique name
        name = parts[1]

        # caps = "capabilities" → set of function types the client can execute.
        # Example: caps={1,3,5} → this client can run tasks of type 1, 3, and 5.
        # The number of caps determines the client's parallel capacity.
        caps = set(map(int, parts[2:]))

        # Register client in the server and store:
        # - capabilities (caps)
        # - TCP writer (for sending tasks)
        # - active tasks (running set)
        await self.register(name, caps, writer)
        client = self.clients[name]

        # Create two async tasks that run independently:
        # 1) sender → constantly checks and sends tasks
        # 2) receiver → receives RESULT messages from the client
        sender = asyncio.create_task(self.client_task_sender(client))
        receiver = asyncio.create_task(self.client_task_receiver(client, reader))

        # Wait until either sender or receiver finishes
        # (e.g., disconnect or no more tasks)
        await asyncio.wait([sender, receiver], return_when=asyncio.FIRST_COMPLETED)

        print(f"[SERVER] Client {name} disconnected.")

        # Mark client as dead and return its running tasks to queues
        client.alive = False
        await self.reassign_running_tasks(client)

        # Close the TCP connection
        writer.close()

    # ----------------------------------------------------------
    async def reassign_running_tasks(self, client: ClientInfo):
        """
        Returns running tasks of a dead client back to pending queues.
        """
        async with self._cond:
            for tid in list(client.running):
                t = self.tasks[tid]
                t.state = "pending"
                t.start_time = None
                t.end_time = None
                self.tasks_by_type[t.func_type].appendleft(t)

            client.running.clear()
            self._cond.notify_all()


# ============================================================
# START TCP SERVER
# ============================================================

async def start_server(server: Server):
    """
    Creates a TCP server and listens forever.
    """
    print("[SERVER] Running on 127.0.0.1:5000...")

    srv = await asyncio.start_server(
        lambda r, w: server.handle_client(r, w),
        "127.0.0.1",
        5000
    )

    async with srv:
        await srv.serve_forever()
