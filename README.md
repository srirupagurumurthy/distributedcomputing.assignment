# Mini Distributed Application - Design Issues Analysis

## Aim: Design a mini distributed app and analyze scalability, communication, failure handling, resource sharing.

## Architecture Implemented
Centralized Lock Manager (Token-based Mutual Exclusion) - Simplest form of Suzuki-Kasami + Lamport ME from CS3551 Syllabus.

[Client-A] [Client-B] [Client-C] -> TCP Socket (TYPE|nodeId|LamportClock) -> [Coordinator:5000] -> shared_file.txt

## Implementations
- **Java**: Enterprise-grade with ExecutorService, ConcurrentHashMap
- **Python**: Lightweight, same logic, easy to demo

## 1. Scalability
- Issue: Coordinator bottleneck.
- Solution: Thread pool (50 threads), ConcurrentLinkedQueue, dynamic node join without restart.
- Test: Ran 3 nodes, can scale to 50.
- Future: Replace with Raymond Tree (O(log n) msgs) or Consistent Hashing.

## 2. Communication
- Issue: Message loss, ordering.
- Solution: TCP (reliable), custom protocol `REQUEST|nodeId|timestamp`, ACK, Lamport Clock `Ci = max(Ci, ts)+1` for total ordering (as per Q7).
- Retry: 3 times on timeout.

## 3. Failure Handling
- Issue: Node crashes holding lock -> deadlock.
- Solution:
  a) Heartbeat every 3 sec, Coordinator timeout 10 sec
  b) If holder crashes -> auto RELEASE and grant to next in queue
  c) Checkpointing: Every lock change logged to checkpoint.log (as per Q4)

## 4. Resource Sharing (Mutual Exclusion)
- Issue: Safety, Liveness, No Starvation.
- Solution: Centralized queue + FIFO by Lamport timestamp. Only GRANTED node can enter CS and write shared_file.txt.
- Proves: Safety (only one in CS), Fairness (queue).

## How to Run

### Python (Recommended for quick demo)
Terminal1: python python-version/coordinator.py
Terminal2: python python-version/node.py Client-A
Terminal3: python python-version/node.py Client-B

### Java
javac java-version/*.java
Terminal1: java -cp java-version Coordinator
Terminal2: java -cp java-version Node Client-A

## Results
shared_file.txt:
Client-A wrote at Tue Nov 04...
Client-B wrote at Tue Nov 04...
-> Proves only one at a time = Mutual Exclusion

checkpoint.log:
Tue Nov 04 Holder=Client-A Queue=1
-> Proves checkpointing & recovery

## Mapping to Syllabus
Q7 Lamport ME, Q2 Suzuki-Kasami, Q4 Coordinated Checkpointing, Q10 Deadlock Detection/Prevention
