import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {
    private static final int PORT = 5000;
    private static final Map<String, Long> nodes = new ConcurrentHashMap<>();
    private static final Queue<Request> lockQueue = new ConcurrentLinkedQueue<>();
    private static String lockHolder = null;
    private static final ExecutorService pool = Executors.newFixedThreadPool(50);
    private static long logicalClock = 0;

    static class Request {
        String nodeId; long timestamp; Socket socket; PrintWriter out;
        Request(String id, long ts, Socket s, PrintWriter o){ nodeId=id; timestamp=ts; socket=s; out=o; }
    }

    public static void main(String[] args) throws IOException {
        ServerSocket server = new ServerSocket(PORT);
        System.out.println("[COORDINATOR] Started on port " + PORT + " - Waiting for nodes...");

        // Heartbeat monitor thread - Failure Handling
        new Thread(() -> {
            while(true){
                try{ Thread.sleep(5000); checkFailures(); } catch(Exception e){}
            }
        }).start();

        while(true){
            Socket client = server.accept();
            pool.execute(() -> handleClient(client));
        }
    }

    private static void handleClient(Socket socket){
        try{
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String line;
            while((line = in.readLine()) != null){
                System.out.println("[RECV] " + line);
                String[] parts = line.split("\\|"); // format: TYPE|nodeId|timestamp
                String type = parts[0];
                String nodeId = parts[1];
                long ts = parts.length>2 ? Long.parseLong(parts[2]) : System.currentTimeMillis();
                logicalClock = Math.max(logicalClock, ts) + 1;
                nodes.put(nodeId, System.currentTimeMillis());

                if(type.equals("HEARTBEAT")){ out.println("ACK|HB|"+logicalClock); }
                else if(type.equals("REQUEST")){
                    synchronized(Coordinator.class){
                        if(lockHolder == null){
                            lockHolder = nodeId;
                            out.println("GRANTED|"+nodeId+"|"+logicalClock);
                            System.out.println("[LOCK] Granted to " + nodeId);
                            logCheckpoint();
                        } else {
                            lockQueue.add(new Request(nodeId, ts, socket, out));
                            out.println("QUEUED|"+nodeId+"|"+logicalClock);
                            System.out.println("[LOCK] Queued " + nodeId + " QueueSize=" + lockQueue.size());
                        }
                    }
                }
                else if(type.equals("RELEASE")){
                    synchronized(Coordinator.class){
                        if(nodeId.equals(lockHolder)){
                            System.out.println("[LOCK] Released by " + nodeId);
                            lockHolder = null;
                            if(!lockQueue.isEmpty()){
                                Request next = lockQueue.poll();
                                lockHolder = next.nodeId;
                                next.out.println("GRANTED|"+next.nodeId+"|"+logicalClock);
                                System.out.println("[LOCK] Granted to next " + next.nodeId);
                            }
                            out.println("RELEASED|"+nodeId+"|"+logicalClock);
                            logCheckpoint();
                        }
                    }
                }
            }
        } catch(Exception e){ System.out.println("Client disconnected: " + e.getMessage()); }
    }

    private static void checkFailures(){
        long now = System.currentTimeMillis();
        for(String nodeId : new ArrayList<>(nodes.keySet())){
            if(now - nodes.get(nodeId) > 10000){ // 10 sec timeout
                System.out.println("[FAILURE] Node failed: " + nodeId + " - Removing");
                nodes.remove(nodeId);
                if(nodeId.equals(lockHolder)){
                    System.out.println("[RECOVERY] Holder crashed, auto-releasing");
                    lockHolder = null;
                    if(!lockQueue.isEmpty()){
                        Request next = lockQueue.poll();
                        lockHolder = next.nodeId;
                        next.out.println("GRANTED|"+next.nodeId+"|"+logicalClock);
                    }
                }
                lockQueue.removeIf(r -> r.nodeId.equals(nodeId));
            }
        }
    }
    private static void logCheckpoint(){
        try(PrintWriter pw = new PrintWriter(new FileWriter("checkpoint.log", true))){
            pw.println(new Date() + " Holder=" + lockHolder + " Queue=" + lockQueue.size());
        } catch(Exception e){}
    }
                  }
