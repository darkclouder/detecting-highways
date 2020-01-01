package highways.utils;

import java.util.HashMap;
import java.util.Map;

public class MultiStopWatch {

    private final HashMap<String, Double> times;
    private final HashMap<String, Double> startTime;

    public MultiStopWatch() {
        times = new HashMap<>();
        startTime = new HashMap<>();
    }

    public void start(final String id) {
        if (!times.containsKey(id)) {
            times.put(id, 0.0);
        }

        final double timestamp = System.nanoTime();

        startTime.put(id, timestamp);
    }

    public void stop(final String id) {
        final double stopTimestamp = System.nanoTime();

        if (!startTime.containsKey(id)) {
            throw new IllegalStateException(id + " was stopped but never started");
        }

        final double startTimestamp = startTime.get(id);

        double NANO_SECONDS = 1_000_000_000.0;
        final double time = times.get(id) + (stopTimestamp - startTimestamp) / NANO_SECONDS;

        times.put(id, time);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Double> watches: times.entrySet()) {
            sb.append(watches.getKey());
            sb.append(": ");
            sb.append(watches.getValue());
            sb.append("\n");
        }

        return sb.toString();
    }
}
