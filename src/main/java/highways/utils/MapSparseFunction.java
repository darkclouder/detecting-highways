package highways.utils;

import java.util.Iterator;
import java.util.Map;

public class MapSparseFunction<A, B> implements SparseFunction<A, B> {
    private final Map<A, B> map;
    private final B defaultValue;
    private final boolean hasDefault;

    public MapSparseFunction(final Map<A, B> map) {
        this.map = map;
        this.defaultValue = null;
        this.hasDefault = false;
    }

    public MapSparseFunction(final Map<A, B> map, final B defaultValue) {
        this.map = map;
        this.defaultValue = defaultValue;
        this.hasDefault = true;
    }

    @Override
    public B apply(A a) {
        if (hasDefault) {
            return map.getOrDefault(a, defaultValue);
        } else {
            return map.get(a);
        }
    }

    @Override
    public Iterable<A> sparse() {
        return map.keySet();
    }

    @Override
    public Iterable<B> sparseApplied() {
        return map.values();
    }

    @Override
    public Iterable<Pair<A, B>> sparsePair() {
        return () -> new Iterator<>() {
            Iterator<Map.Entry<A, B>> entries = map.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public Pair<A, B> next() {
                return Pair.toPair(entries.next());
            }
        };
    }
}
