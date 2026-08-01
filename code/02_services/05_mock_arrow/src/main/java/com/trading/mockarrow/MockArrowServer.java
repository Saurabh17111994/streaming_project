package com.trading.mockarrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock Arrow WebSocket server — emits fake market tick data.
 *
 * All prices in integer paise (₹1 = 100 paise).
 * Starts a plain WebSocket server on port 8888. Connected clients receive
 * realistic tick messages at configurable rate. Uses a fixed instrument universe
 * with random price walks. No real broker connection needed.
 *
 * Message format (newline-delimited JSON):
 *   {"instrument_token": 12345, "exchange_ts": 1700000000000, "last_price_paise": 15150, ...}
 */
public class MockArrowServer {
    private static final Logger log = LoggerFactory.getLogger(MockArrowServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long DEFAULT_SEED = 20260801L;

    private final int port;
    private final int tickRatePerSec;
    private final List<Long> instruments;
    private final SyntheticWorkload workload;
    private final AtomicLong tickCounter = new AtomicLong(0);
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private ScheduledExecutorService tickScheduler;

    // Per-instrument price state for realistic walks (in paise)
    private final Map<Long, Long> prices = new ConcurrentHashMap<>();
    private final Map<Long, Long> basePrices = new HashMap<>();

    public MockArrowServer(int port, int tickRatePerSec, Collection<Long> instruments) {
        this(port, tickRatePerSec, instruments, DEFAULT_SEED);
    }

    public MockArrowServer(int port, int tickRatePerSec, Collection<Long> instruments, long seed) {
        this.port = port;
        this.tickRatePerSec = tickRatePerSec;
        this.instruments = List.copyOf(instruments);
        SyntheticWorkload.Profile profile = tickRatePerSec >= 30
                ? SyntheticWorkload.Profile.PEAK : SyntheticWorkload.Profile.BASELINE;
        this.workload = new SyntheticWorkload(new SyntheticWorkload.Config(
                this.instruments, seed, profile, System.currentTimeMillis()));
        SplittableRandom rng = new SplittableRandom(seed);
        for (long inst : instruments) {
            long base = 10000L + rng.nextLong(190001); // 100.00-2000.01 rupees
            basePrices.put(inst, base);
            prices.put(inst, base);
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        log.info("Mock Arrow WebSocket server started on ws://0.0.0.0:{} ({} instruments, {} ticks/s)",
                 port, instruments.size(), tickRatePerSec);

        // Accept connections in background
        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    log.info("Client connected: {}", client.getRemoteSocketAddress());
                    handleClient(client);
                } catch (IOException e) {
                    if (running) log.error("Accept error", e);
                }
            }
        }, "mock-arrow-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        // Schedule tick generation
        long intervalMs = 10;
        tickScheduler = Executors.newSingleThreadScheduledExecutor();
        tickScheduler.scheduleAtFixedRate(this::generateTicks, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    // Connected clients
    private final List<ClientSession> clients = new CopyOnWriteArrayList<>();

    private record ClientSession(Socket socket, BufferedWriter writer, long connectedAt) {}

    private void handleClient(Socket client) {
        var session = new ClientSession(client, null, System.currentTimeMillis());
        try {
            var writer = new BufferedWriter(
                new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
            var realSession = new ClientSession(client, writer, session.connectedAt);
            clients.add(realSession);
        } catch (IOException e) {
            log.error("Failed to setup client writer", e);
        }
    }

    private void generateTicks() {
        if (clients.isEmpty()) return;
        int batchSize = Math.max(1, (int) Math.ceil(instruments.size() * tickRatePerSec / 100.0));
        List<Map<String, Object>> batch = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            long inst = workload.next().instrumentToken();
            long pricePaise = walkPrice(inst);
            long now = System.currentTimeMillis();

            Map<String, Object> tick = new LinkedHashMap<>();
            tick.put("instrument_token", inst);
            tick.put("exchange_ts", now);
            tick.put("last_price_paise", pricePaise);
            tick.put("last_qty", 1 + ThreadLocalRandom.current().nextInt(100));
            tick.put("change_pct", round2((pricePaise - basePrices.get(inst)) * 100.0 / basePrices.get(inst)));
            tick.put("volume", 1000L + ThreadLocalRandom.current().nextInt(50000));
            tick.put("buy_quantity", 100 + ThreadLocalRandom.current().nextInt(5000));
            tick.put("sell_quantity", 100 + ThreadLocalRandom.current().nextInt(5000));
            tick.put("ohlc_open_paise", pricePaise - ThreadLocalRandom.current().nextInt(201));
            tick.put("ohlc_high_paise", pricePaise + ThreadLocalRandom.current().nextInt(301));
            tick.put("ohlc_low_paise", pricePaise - ThreadLocalRandom.current().nextInt(301));
            tick.put("ohlc_close_paise", pricePaise);
            tick.put("depth_buy", generateDepth(pricePaise, -1, 5));
            tick.put("depth_sell", generateDepth(pricePaise + 5, 1, 5));
            batch.add(tick);
        }

        String json;
        try {
            json = mapper.writeValueAsString(batch);
        } catch (Exception e) {
            log.error("JSON serialization failed", e);
            return;
        }
        tickCounter.addAndGet(batch.size());

        // Send to all connected clients
        var deadClients = new ArrayList<ClientSession>();
        for (var session : clients) {
            try {
                session.writer.write(json);
                session.writer.newLine();
                session.writer.flush();
            } catch (IOException e) {
                deadClients.add(session);
            }
        }
        clients.removeAll(deadClients);
        for (var dead : deadClients) {
            try { dead.socket.close(); } catch (IOException ignored) {}
        }
    }

    /** Returns price in paise after a random walk step. */
    private long walkPrice(long inst) {
        return prices.compute(inst, (k, v) -> {
            long base = basePrices.get(inst);
            long step = (long) (ThreadLocalRandom.current().nextGaussian() * 50); // ~0.50 rupees std dev
            return Math.max(base / 2, Math.min(base * 2, v + step));
        });
    }

    /** Generate depth levels in paise. direction: -1 = buy (below mid), 1 = sell (above mid) */
    private List<Map<String, Object>> generateDepth(long midPricePaise, int direction, int levels) {
        var depth = new ArrayList<Map<String, Object>>(levels);
        for (int i = 0; i < levels; i++) {
            long offset = 5L * (i + 1); // 0.05 rupees = 5 paise per level
            Map<String, Object> level = new LinkedHashMap<>();
            level.put("price_paise", midPricePaise + direction * offset);
            level.put("qty", 100 + ThreadLocalRandom.current().nextInt(5000));
            depth.add(level);
        }
        return depth;
    }

    private double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    public long getTickCount() { return tickCounter.get(); }

    public void stop() {
        running = false;
        if (tickScheduler != null) tickScheduler.shutdownNow();
        for (var c : clients) {
            try { c.writer.close(); } catch (IOException ignored) {}
            try { c.socket.close(); } catch (IOException ignored) {}
        }
        clients.clear();
        try { serverSocket.close(); } catch (IOException ignored) {}
        log.info("Mock Arrow server stopped ({} ticks emitted)", tickCounter.get());
    }

    // --- Main entry point ---
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("MOCK_ARROW_PORT", "8888"));
        String profileName = System.getenv().getOrDefault("MOCK_ARROW_PROFILE", "baseline");
        SyntheticWorkload.Profile profile;
        try {
            profile = SyntheticWorkload.Profile.valueOf(profileName.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown MOCK_ARROW_PROFILE: " + profileName, e);
        }
        long seed = Long.parseLong(requireEnv("MOCK_ARROW_SEED"));
        int rate = profile == SyntheticWorkload.Profile.PEAK ? 30 : 20;
        int numInstruments = Integer.parseInt(
            System.getenv().getOrDefault("MOCK_ARROW_INSTRUMENTS", "50"));

        List<Long> instruments = new ArrayList<>();
        if (numInstruments <= 0) throw new IllegalArgumentException("MOCK_ARROW_INSTRUMENTS must be positive");
        for (int i = 0; i < numInstruments; i++) {
            instruments.add(100000L + i * 100L);
        }

        var server = new MockArrowServer(port, rate, instruments, seed);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        while (true) Thread.sleep(1000);
    }

    private static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value;
    }
}
