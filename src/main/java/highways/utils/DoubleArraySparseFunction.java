package highways.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

public class DoubleArraySparseFunction implements SparseFunction<Integer, Double> {
    private final double[] array;
    private final double defaultValue;
    private final boolean hasDefault;

    public DoubleArraySparseFunction(final double[] array) {
        this.array = array;
        this.defaultValue = 0;
        this.hasDefault = false;
    }

    public DoubleArraySparseFunction(final double[] array, final double defaultValue) {
        this.array = array;
        this.defaultValue = defaultValue;
        this.hasDefault = true;
    }

    @Override
    public Double apply(Integer index) {
        if (hasDefault) {
            if (index >= 0 && index < array.length) {
                return array[index];
            } else {
                return defaultValue;
            }
        }

        return array[index];
    }

    @Override
    public Iterable<Integer> sparse() {
        return () -> IntStream.range(0, array.length).iterator();
    }

    @Override
    public Iterable<Double> sparseApplied() {
        return () -> Arrays.stream(array).mapToObj(Double::valueOf).iterator();
    }

    @Override
    public Iterable<Pair<Integer, Double>> sparsePair() {
        return () -> new Iterator<>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < array.length;
            }

            @Override
            public Pair<Integer, Double> next() {
                final Pair<Integer, Double> p = new Pair<>(i, array[i]);
                i++;

                return p;
            }
        };
    }

}
