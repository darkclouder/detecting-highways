package highways.utils;

import java.util.Iterator;
import java.util.function.Function;

public class SimpleSparseFunction<A, B> implements SparseFunction<A, B> {
    private final Function<A,B> f;
    private final Iterable<A> v;

    public SimpleSparseFunction(Function<A, B> f, Iterable<A> v) {
        this.f = f;
        this.v = v;
    }

    @Override
    public B apply(A a) {
        return f.apply(a);
    }

    @Override
    public Iterable<A> sparse() {
        return v;
    }

    @Override
    public Iterable<B> sparseApplied() {
        return () -> {
            Iterator<A> it = v.iterator();

            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public B next() {
                    return f.apply(it.next());
                }
            };
        };
    }

    @Override
    public Iterable<Pair<A, B>> sparsePair() {
        return () -> {
            Iterator<A> it = v.iterator();

            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Pair<A, B> next() {
                    final A value = it.next();

                    return new Pair<>(value, f.apply(value));
                }
            };
        };
    }
}
