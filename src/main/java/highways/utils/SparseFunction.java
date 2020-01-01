package highways.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public interface SparseFunction<A, B> extends Function<A, B> {
    Iterable<A> sparse();
    Iterable<B> sparseApplied();
    Iterable<Pair<A, B>> sparsePair();

    default boolean equalMapping(SparseFunction<A, B> other) {
        for (A a: sparse()) {
            try {
                if (!apply(a).equals(other.apply(a))) {
                    return false;
                }
            } catch(Exception e) {
                return false;
            }
        }

        for (A a: other.sparse()) {
            try {
                if (!apply(a).equals(other.apply(a))) {
                    return false;
                }
            } catch(Exception e) {
                return false;
            }
        }

        return true;
    }

    default Map<A, Pair<B, B>> differentMapping(SparseFunction<A, B> other) {
        Map<A, Pair<B, B>> diff = new HashMap<>();

        for (A a: sparse()) {
            if (!apply(a).equals(other.apply(a))) {
                diff.put(a, new Pair<>(apply(a), other.apply(a)));
            }
        }

        for (A a: other.sparse()) {
            if (!diff.containsKey(a)) {
                if (!apply(a).equals(other.apply(a))) {
                    diff.put(a, new Pair<>(apply(a), other.apply(a)));
                }
            }
        }

        return diff;
    }
}

