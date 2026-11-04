import java.io.*;
import java.net.*;
import java.util.Random;

public class Node {
    private String nodeId;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private long clock = 0;

    public Node(String id){ this.nodeId = id; }

    public void start() throws Exception {
        socket = new Socket("localhost", 5000);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        System.out.println("["+nodeId+"] Connected to Coordinator");

        // Heartbeat thread
        new Thread(() -> {
            while(true){
                try{ Thread.sleep(3000); clock++; out.println("HEARTBEAT|"+nodeId+"|"+clock); } catch(Exception e){ break; }
            }
        }).start();

        // Listen for GRANTED
        new Thread(() -> {
            try{
                String line;
                while((line = in.readLine()) != null){
                    System.out.println("["+nodeId+" RECV] " + line);
                    if(line.startsWith("GRANTED")){
                        enterCriticalSection();
                    }
                }
            }catch(Exception e){}
        }).start();

        // Simulate random requests
        Random r = new Random();
        while(true){
            Thread.sleep(5000 + r.nextInt(5000));
            requestResource();
        }
    }

    private void requestResource(){
        clock++;
        System.out.println("["+nodeId+"] --> REQUEST at clock " + clock);
        out.println("REQUEST|"+nodeId+"|"+clock);
    }

    private void enterCriticalSection() throws Exception {
        System.out.println("["+nodeId+"] *** ENTERING CS - Writing to shared file ***");
        synchronized(Node.class){
            FileWriter fw = new FileWriter("shared_file.txt", true);
            fw.write(nodeId + " wrote at " + new java.util.Date() + " clock=" + clock + "\n");
            fw.close();
        }
        Thread.sleep(2000); // work in CS
        System.out.println("["+nodeId+"] *** LEAVING CS ***");
        clock++;
        out.println("RELEASE|"+nodeId+"|"+clock);
    }

    public static void main(String[] args) throws Exception {
        String id = args.length>0 ? args[0] : "Node-" + (int)(Math.random()*100);
        new Node(id).start();
    }
}
