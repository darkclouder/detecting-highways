package highways.utils;

import java.util.Map;
import java.util.Objects;

public class Pair<V, W> {
    public final V x;
    public final W y;

    public Pair(final V x, final W y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof Pair<?, ?>)) {
            return false;
        }

        Pair<?, ?> otherPair = (Pair<?, ?>)other;

        return x.equals(otherPair.x) && y.equals(otherPair.y);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", x.toString(), y.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }

    public boolean contains(final Object value) {
        return x.equals(value) || y.equals(value);
    }

    public Pair<W, V> revert() {
        return new Pair<>(y, x);
    }

    public static <A, B> Pair<A, B> toPair(Map.Entry<A, B> entry) {
        return new Pair<>(entry.getKey(), entry.getValue());
    }
}
