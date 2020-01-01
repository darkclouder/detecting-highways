package highways.utils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ProgressCounter {
    final long total;
    final long stepSize;
    final AtomicLong counter = new AtomicLong(0);
    long startTime;
    long lastCheckpoint;
    double slidingStepTime;

    public ProgressCounter(final long total) {
        this.total = total;
        stepSize = Math.max(1, total / 1000);
    }

    public void start() {
        startTime = System.currentTimeMillis();
        lastCheckpoint = startTime;
    }

    public <T> Function<T, T> relayCount() {
        return input -> {
            count();
            return input;
        };
    }

    public void count() {
        final long currCount = counter.incrementAndGet();

        if (currCount % stepSize == 0) {
            final long now = System.currentTimeMillis();
            final long stepTime = now - lastCheckpoint;
            final long elapsed = now - startTime;
            final float frac = (float)currCount / total;

            slidingStepTime = 0.5 * slidingStepTime + 0.5 * stepTime;
            lastCheckpoint = now;

            final double expectedLeft = (float)(total - currCount) / stepSize * slidingStepTime;

            System.out.println(String.format(
                    "%f%% (elapsed time: %ds, est. remaining time: %ds)",
                    frac * 100,
                    elapsed / 1000,
                    Math.round(expectedLeft / 1000)
            ));
        }

    }
}
