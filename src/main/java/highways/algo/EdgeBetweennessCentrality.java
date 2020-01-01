package highways.algo;

import grph.Grph;
import highways.utils.MapSparseFunction;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Stack;

public class EdgeBetweennessCentrality extends BetweennessCentrality {
    public EdgeBetweennessCentrality(final Grph g, final SparseFunction<Integer, Double> weights) {
        super(g, weights);
    }

    @Override
    public SparseFunction<Integer, Double> compute() {
        final int m = g.getNumberOfEdges();

        final Int2DoubleMap betweenness = new Int2DoubleOpenHashMap(m);
        betweenness.defaultReturnValue(0.0);

        for (final int s: g.getVertices()) {
            accumulate(betweenness, s);
        }

        return new MapSparseFunction<>(betweenness);
    }

    @Override
    Int2DoubleMap computePartial(final int s) {
        final int m = g.getNumberOfEdges();

        final Int2DoubleMap partialBetweenness = new Int2DoubleOpenHashMap(m);
        partialBetweenness.defaultReturnValue(0.0);
        accumulate(partialBetweenness, s);

        return partialBetweenness;
    }

    private void accumulate(
            final Int2DoubleMap betweenness,
            final int s
    ) {
        final int n = g.getNumberOfVertices();
        final Pair<Pair<IntList[], Stack<Integer>>, int[]> result = computeExploration(s);
        final IntList[] pred = result.x.x;
        final Stack<Integer> S = result.x.y;
        final int[] sigma = result.y;

        final Int2DoubleMap d = new Int2DoubleOpenHashMap(n);
        d.defaultReturnValue(0.0);

        while (!S.isEmpty()) {
            final int w = S.pop();

            for (final int v: pred[w]) {
                final double c = (double)sigma[v] / (double)sigma[w] * (1.0 + d.get(w));

                final IntSet edges = g.getEdgesConnecting(v, w);

                if (edges.size() != 1) {
                    throw new IllegalStateException("Does not allow multi-edges");
                }

                final int e = edges.iterator().nextInt();

                d.put(v, d.get(v) + c);
                betweenness.put(e, betweenness.get(e) + c);
            }
        }
    }
}
