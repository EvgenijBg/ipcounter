import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IpCounter {
    static final BitSet first = new BitSet(Integer.MAX_VALUE);
    static final BitSet second = new BitSet(Integer.MAX_VALUE);
    //Adjust performance here
    static final int CHUNK_SIZE = (1024 * 1024) * 10; //Megabytes per chunk
    public static final int THREADS = 10;

    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        String path;
        boolean runNaiveComparison;
        try (Scanner input = new Scanner(System.in)) {
            System.out.print("Provide file path: ");
            path = input.nextLine();
            System.out.print("Run comparison with naive approach after? [Y/n]: ");
            runNaiveComparison = "y".equalsIgnoreCase(input.nextLine());
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        try (FileReader fr = new FileReader(path)) {
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            while (true) {
                String chunk = readChunk(fr);
                if (chunk != null) {
                    executor.execute(new ChunkProcessor(chunk));
                } else break;
            }

            executor.shutdown();
            while (!executor.isTerminated()) {
                // Wait until all tasks are finished
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Unique IP addresses:" + (first.cardinality() + second.cardinality()));
        System.out.println("Time of execution:" + (System.currentTimeMillis() - time));
        if (runNaiveComparison) {
            System.out.println("Naive approach comparison running...");
            time = System.currentTimeMillis();
            HashSet<String> ips = new HashSet<>();
            try (Scanner sc = new Scanner(new FileInputStream(path))) {
                while (sc.hasNext()) {
                    ips.add(sc.nextLine().trim());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Naive approach count: " + ips.size());
            System.out.println("Naive approach time: " + (System.currentTimeMillis() - time));
        }
    }

    public static String readChunk(FileReader fr) throws IOException {
        char[] buffer = new char[CHUNK_SIZE];

        fr.read(buffer, 0, CHUNK_SIZE - 30);
        for (int i = 0; i < 30; i++) {
            byte read = (byte) fr.read();
            if (read == -1) {
                break;
            }
            if ((char) read == '\r' || (char) read == '\n') {
                //Avoid breaking address
                break;
            }
            buffer[CHUNK_SIZE - 30 + i] = (char) read;

        }
        String ret = new String(buffer).trim();
        return ret.length() > 0 ? ret : null;
    }

    private static long ipToLong(String ipAddress) {

        try {
            long result = 0;
            String[] ipParts = ipAddress.split("\\.");

            for (int i = 0; i < 4; i++) {
                result |= (Long.parseLong(ipParts[i]) << (24 - (8 * i)));
            }
            return result;

        } catch (Exception e) {
            System.out.println(ipAddress);
            throw e;
        }
    }


    static class ChunkProcessor implements Runnable {
        private final String chunk;

        public ChunkProcessor(String chunk) {
            this.chunk = chunk;
        }

        @Override
        public void run() {
            final BitSet localFirst = new BitSet(Integer.MAX_VALUE);
            final BitSet localSecond = new BitSet(Integer.MAX_VALUE);
            try (Scanner chunkScanner = new Scanner(chunk)) {
                while (chunkScanner.hasNext()) {
                    long ip = ipToLong(chunkScanner.nextLine());
                    if (ip > Integer.MAX_VALUE) {
                        localSecond.set((int) (ip - Integer.MAX_VALUE), true);
                    } else {
                        localFirst.set((int) ip, true);
                    }
                }
                synchronized (first) {
                    first.or(localFirst);
                }
                synchronized (second) {
                    second.or(localSecond);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
