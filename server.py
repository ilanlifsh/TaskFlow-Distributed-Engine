import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional, Set, Deque
from collections import deque

from protocol import (
    encode_task,
    encode_no_tasks,
    parse_message,
    is_result
)

TASK_MAX_WAIT = 60
TASK_EXEC_TIMES = {1:1,2:2,3:3,4:4,5:5}

# ------------------------------------------------------------
# Task Structures
# ------------------------------------------------------------

@dataclass
class Task:
    id: int
    func_type: int
    param: int
    arrival: float
    deadline: float
    assigned_to: Optional[str] = None
    state: str = "pending"
    start_time: Optional[float] = None
    end_time: Optional[float] = None

@dataclass
class ClientInfo:
    name: str
    caps: Set[int]
    running: Dict[int, int]
    writer: asyncio.StreamWriter
    capacity: int
    alive: bool = True

# ------------------------------------------------------------
# Server
# ------------------------------------------------------------

class Server:

    def __init__(self):
        self.tasks_by_type: Dict[int, Deque[Task]] = {t: deque() for t in TASK_EXEC_TIMES}
        self.tasks: Dict[int, Task] = {}
        self.clients: Dict[str, ClientInfo] = {}
        self.end_file = False
        self._next_id = 1
        self._cond = asyncio.Condition()

    # ---- Add task ----
    async def add_task(self, func, param):
        async with self._cond:
            now = time.time()
            t = Task(
                id=self._next_id,
                func_type=func,
                param=param,
                arrival=now,
                deadline=now + TASK_MAX_WAIT
            )
            self._next_id += 1

            self.tasks[t.id] = t
            self.tasks_by_type[func].append(t)

            self._cond.notify_all()

    async def mark_end(self):
        async with self._cond:
            self.end_file = True
            self._cond.notify_all()

    # ---- Register client ----
    async def register(self, name, caps, writer):
        async with self._cond:
            info = ClientInfo(
                name=name,
                caps=caps,
                running={},
                writer=writer,
                capacity=len(caps)
            )
            self.clients[name] = info
            print(f"[SERVER] {name} registered with caps={caps}")
            self._cond.notify_all()

    # ---- Cleanup expired ----
    def cleanup_expired(self, func):
        q = self.tasks_by_type[func]
        now = time.time()

        while q and q[0].deadline < now:
            t = q.popleft()
            if t.state == "pending":
                t.state = "discarded"
                t.end_time = now
                print(f"[SERVER] Discarded task {t.id}")

    def cleanup_all_expired(self):
        for f in self.tasks_by_type:
            self.cleanup_expired(f)

    def _all_tasks_done(self):
        return self.tasks and all(
            t.state in ("completed", "discarded") for t in self.tasks.values()
        )

    # ---- Task selection ----
    async def select_task(self, client):
        while True:
            async with self._cond:

                if not client.alive:
                    return None

                if len(client.running) >= client.capacity:
                    await self._cond.wait()
                    continue

                best_task = None
                best_len = 999_999

                for f in client.caps:
                    if f in client.running:
                        continue

                    q = self.tasks_by_type[f]
                    if q and len(q) < best_len:
                        best_len = len(q)
                        best_task = q[0]

                if best_task:
                    self.tasks_by_type[best_task.func_type].popleft()
                    best_task.assigned_to = client.name
                    best_task.start_time = time.time()
                    best_task.state = "running"

                    client.running[best_task.func_type] = best_task.id
                    return best_task

                if self.end_file and self._all_tasks_done():
                    return None

                await self._cond.wait()

    # ---- Handle results ----
    async def report(self, client_name, tid, func, value):
        async with self._cond:
            t = self.tasks[tid]
            c = self.clients[client_name]

            t.state = "completed"
            t.end_time = time.time()

            if func in c.running:
                del c.running[func]

            print(f"[SERVER] Task {tid} completed by {client_name} val={value}")
            self._cond.notify_all()

    # ---- Sender ----
    async def client_task_sender(self, client):
        while client.alive:
            task = await self.select_task(client)

            if task is None:
                client.writer.write(encode_no_tasks().encode())
                await client.writer.drain()

                while client.running:
                    await asyncio.sleep(0.1)

                client.alive = False
                return

            client.writer.write(
                encode_task(task.id, task.func_type, task.param).encode()
            )
            await client.writer.drain()

    # ---- Receiver ----
    async def client_task_receiver(self, client, reader):
        while client.alive:
            line = await reader.readline()
            if not line:
                client.alive = False
                return

            parts = parse_message(line.decode())
            if is_result(parts):
                tid = int(parts[1])
                name = parts[2]
                func = int(parts[3])
                value = int(parts[4])
                await self.report(name, tid, func, value)

    # ---- Connection handler ----
    async def handle_client(self, reader, writer):
        line = await reader.readline()
        parts = parse_message(line.decode())

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

    # ---- Reassign tasks ----
    async def reassign_running_tasks(self, client):
        async with self._cond:
            now = time.time()

            for func, tid in list(client.running.items()):
                t = self.tasks[tid]
                t.state = "pending"
                t.assigned_to = None
                t.start_time = None
                t.end_time = None
                t.deadline = now + TASK_MAX_WAIT

                self.tasks_by_type[t.func_type].appendleft(t)

            client.running.clear()
            self._cond.notify_all()

    # ---- Expiration ----
    async def expiration_loop(self):
        while True:
            async with self._cond:
                self.cleanup_all_expired()
                self._cond.notify_all()

                if self.end_file and self._all_tasks_done():
                    return

            await asyncio.sleep(1)


async def start_server(server):
    print("[SERVER] Listening on 127.0.0.1:5000")
    asyncio.create_task(server.expiration_loop())

    srv = await asyncio.start_server(
        lambda r, w: server.handle_client(r, w),
        "127.0.0.1",
        5000
    )

    async with srv:
        await srv.serve_forever()
