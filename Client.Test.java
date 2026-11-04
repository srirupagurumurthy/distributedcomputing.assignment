public class ClientTest {
    public static void main(String[] args) throws Exception {
        // Launch 3 nodes to show mutual exclusion
        new Thread(() -> { try{ new Node("Client-A").start(); } catch(Exception e){} }).start();
        Thread.sleep(1000);
        new Thread(() -> { try{ new Node("Client-B").start(); } catch(Exception e){} }).start();
        Thread.sleep(1000);
        new Thread(() -> { try{ new Node("Client-C").start(); } catch(Exception e){} }).start();
    }
}
