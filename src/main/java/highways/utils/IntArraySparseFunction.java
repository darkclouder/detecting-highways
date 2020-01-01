package highways.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

public class IntArraySparseFunction implements SparseFunction<Integer, Integer> {
    private final int[] array;
    private final int defaultValue;
    private final boolean hasDefault;

    public IntArraySparseFunction(final int[] array) {
        this.array = array;
        this.defaultValue = 0;
        this.hasDefault = false;
    }

    public IntArraySparseFunction(final int[] array, final int defaultValue) {
        this.array = array;
        this.defaultValue = defaultValue;
        this.hasDefault = true;
    }

    @Override
    public Integer apply(Integer index) {
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
    public Iterable<Integer> sparseApplied() {
        return () -> Arrays.stream(array).mapToObj(Integer::valueOf).iterator();
    }

    @Override
    public Iterable<Pair<Integer, Integer>> sparsePair() {
        return () -> new Iterator<>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < array.length;
            }

            @Override
            public Pair<Integer, Integer> next() {
                final Pair<Integer, Integer> p = new Pair<>(i, array[i]);
                i++;

                return p;
            }
        };
    }

}
