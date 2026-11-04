import socket, threading, time, json
from collections import deque
from datetime import datetime

PORT = 5000
nodes = {}  # nodeId -> last_heartbeat
lock_holder = None
lock_queue = deque()  # (nodeId, conn)
logical_clock = 0
lock = threading.Lock()

def log_checkpoint():
    with open("checkpoint.log", "a") as f:
        f.write(f"{datetime.now()} Holder={lock_holder} Queue={len(lock_queue)}\n")

def check_failures():
    while True:
        time.sleep(5)
        now = time.time()
        with lock:
            for node_id in list(nodes.keys()):
                if now - nodes[node_id] > 10: # 10 sec timeout
                    print(f"[FAILURE] Node {node_id} crashed!")
                    global lock_holder
                    nodes.pop(node_id, None)
                    if lock_holder == node_id:
                        print("[RECOVERY] Holder crashed, auto-releasing")
                        lock_holder = None
                        if lock_queue:
                            next_id, next_conn = lock_queue.popleft()
                            lock_holder = next_id
                            try: next_conn.send(b"GRANTED|"+next_id.encode()+b"\n")
                            except: pass
                    # remove from queue
                    for item in list(lock_queue):
                        if item[0] == node_id: lock_queue.remove(item)

def handle_client(conn, addr):
    global logical_clock, lock_holder
    print(f"[CONNECTED] {addr}")
    try:
        while True:
            data = conn.recv(1024).decode().strip()
            if not data: break
            for line in data.split("\n"):
                if not line: continue
                print(f"[RECV] {line}")
                parts = line.split("|")
                typ = parts[0]
                node_id = parts[1] if len(parts)>1 else "unknown"
                ts = int(parts[2]) if len(parts)>2 else 0
                
                with lock:
                    logical_clock = max(logical_clock, ts) + 1
                    nodes[node_id] = time.time()

                    if typ == "HEARTBEAT":
                        conn.send(f"ACK|HB|{logical_clock}\n".encode())
                    elif typ == "REQUEST":
                        if lock_holder is None:
                            lock_holder = node_id
                            conn.send(f"GRANTED|{node_id}|{logical_clock}\n".encode())
                            print(f"[LOCK] Granted to {node_id}")
                            log_checkpoint()
                        else:
                            lock_queue.append((node_id, conn))
                            conn.send(f"QUEUED|{node_id}|{logical_clock}\n".encode())
                            print(f"[LOCK] Queued {node_id} size={len(lock_queue)}")
                    elif typ == "RELEASE":
                        if lock_holder == node_id:
                            print(f"[LOCK] Released by {node_id}")
                            lock_holder = None
                            if lock_queue:
                                nxt_id, nxt_conn = lock_queue.popleft()
                                lock_holder = nxt_id
                                nxt_conn.send(f"GRANTED|{nxt_id}|{logical_clock}\n".encode())
                                print(f"[LOCK] Granted to next {nxt_id}")
                            conn.send(f"RELEASED|{node_id}|{logical_clock}\n".encode())
                            log_checkpoint()
    except Exception as e:
        print(f"[DISCONNECT] {addr} {e}")
    finally:
        conn.close()

# Start failure detector
threading.Thread(target=check_failures, daemon=True).start()

s = socket.socket()
s.bind(("localhost", PORT))
s.listen(50)
print(f"[COORDINATOR] Running on port {PORT} | Scalability: ThreadPool 50")
print("Concepts: Centralized ME + Lamport Clock + Heartbeat + Checkpoint")

while True:
    conn, addr = s.accept()
    threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
