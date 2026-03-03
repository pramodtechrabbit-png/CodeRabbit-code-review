import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TradeProcessor {

    private final Object lock = new Object(); // single lock for deadlock fix
    private final List<String> tradeCache = Collections.synchronizedList(new ArrayList<>());
    private KafkaConsumer<String, String> consumer;
    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private volatile boolean running = true; // for graceful shutdown

    private static final int MAX_CACHE_SIZE = 1000; // bounded cache example

    // Process a trade safely
    public void processTrade(String tradeId) {
        synchronized (lock) {
            System.out.println("Processing trade: " + tradeId);
        }
    }

    // Reverse process safely (lock order consistent)
    public void reverseProcess(String tradeId) {
        synchronized (lock) {
            System.out.println("Reversing trade: " + tradeId);
        }
    }

    // Cache trade safely with bounded size
    public void cacheTrade(String trade) {
        synchronized (tradeCache) {
            if (tradeCache.size() >= MAX_CACHE_SIZE) {
                tradeCache.remove(0); // remove oldest entry
            }
            tradeCache.add(trade);
        }
    }

    // Clear cache safely
    public void clearCache() {
        synchronized (tradeCache) {
            tradeCache.clear();
        }
    }

    // Kafka consumer loop with batch commit and graceful shutdown
    public void startConsumer() {
        consumer.subscribe(Arrays.asList("trades-topic"));

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    if (value != null && value.contains("BUY")) {
                        cacheTrade(value);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync(); // commit once per batch
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    // Calculate profit/loss using BigDecimal
    public BigDecimal calculatePnL(BigDecimal buyPrice, BigDecimal sellPrice, int quantity) {
        return sellPrice.subtract(buyPrice)
                        .multiply(BigDecimal.valueOf(quantity))
                        .setScale(2, RoundingMode.HALF_EVEN);
    }

    // Async processing with shared executor
    public void asyncProcess(Runnable task) {
        executor.submit(() -> {
            try {
                task.run();
            } catch (Exception e) {
                Thread.currentThread().interrupt(); // restore interrupt if needed
                e.printStackTrace();
            }
        });
    }

    // Shutdown method for executor and consumer
    public void shutdown() {
        running = false; // stops Kafka loop
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}