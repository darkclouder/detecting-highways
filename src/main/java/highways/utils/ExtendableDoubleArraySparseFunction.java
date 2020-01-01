package highways.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.IntStream;

public class ExtendableDoubleArraySparseFunction implements SparseFunction<Integer, Double> {
    private final ArrayList<Double> array;
    private final double defaultValue;
    private final boolean hasDefault;

    public ExtendableDoubleArraySparseFunction(final ArrayList<Double> array) {
        this.array = array;
        this.defaultValue = 0;
        this.hasDefault = false;
    }

    public ExtendableDoubleArraySparseFunction(final ArrayList<Double> array, final double defaultValue) {
        this.array = array;
        this.defaultValue = defaultValue;
        this.hasDefault = true;
    }

    @Override
    public Double apply(Integer index) {
        if (hasDefault) {
            if (index >= 0 && index < array.size()) {
                return array.get(index);
            } else {
                return defaultValue;
            }
        }

        return array.get(index);
    }

    @Override
    public Iterable<Integer> sparse() {
        return () -> IntStream.range(0, array.size()).iterator();
    }

    @Override
    public Iterable<Double> sparseApplied() {
        return array;
    }

    @Override
    public Iterable<Pair<Integer, Double>> sparsePair() {
        return () -> new Iterator<>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < array.size();
            }

            @Override
            public Pair<Integer, Double> next() {
                final Pair<Integer, Double> p = new Pair<>(i, array.get(i));
                i++;

                return p;
            }
        };
    }

    public void set(final int key, final double value) {
        if (key > array.size() || key < 0) {
            throw new ArrayIndexOutOfBoundsException("Can only add a new key at boundary");
        } else if (key == array.size()) {
            array.add(value);
        } else {
            array.set(key, value);
        }
    }
}
