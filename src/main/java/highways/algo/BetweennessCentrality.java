package highways.algo;

import grph.Grph;
import highways.utils.MapSparseFunction;
import highways.utils.Pair;
import highways.utils.ProgressCounter;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.*;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

public class BetweennessCentrality {
    protected final Grph g;
    protected final SparseFunction<Integer, Double> weights;

    public BetweennessCentrality(final Grph g, final SparseFunction<Integer, Double> weights) {
        this.g = g;
        this.weights = weights;
    }

    public SparseFunction<Integer, Double> compute() {
        final int n = g.getNumberOfVertices();

        final Int2DoubleMap betweenness = new Int2DoubleOpenHashMap(n);
        betweenness.defaultReturnValue(0.0);

        for (final int s: g.getVertices()) {
            accumulate(betweenness, s);
        }

        return new MapSparseFunction<>(betweenness);
    }

    Int2DoubleMap computePartial(final int s) {
        final int n = g.getNumberOfVertices();

        final Int2DoubleMap partialBetweenness = new Int2DoubleOpenHashMap(n);
        partialBetweenness.defaultReturnValue(0.0);
        accumulate(partialBetweenness, s);

        return partialBetweenness;
    }

    Pair<Pair<IntList[], Stack<Integer>>, int[]> computeExploration(final int s) {
        final int n = g.getNumberOfVertices();

        // Initialization
        final boolean[] settled = new boolean[n];
        final double[] totalWeight = new double[n];
        final int[] sigma = new int[n];

        final IntList[] pred = new IntArrayList[n];

        for (int i = 0; i < n; i++) {
            totalWeight[i] = Double.POSITIVE_INFINITY;
            pred[i] = new IntArrayList(1);
        }

        final Stack<Integer> S = new Stack<>();

        Queue<Pair<Integer, Double>> Q = new PriorityQueue<>(Comparator.comparingDouble(o -> o.y));

        totalWeight[s] = 0.0;
        sigma[s] = 1;

        Q.add(new Pair<>(s, totalWeight[s]));

        while (!Q.isEmpty()) {
            final int v = Q.remove().x;

            if (settled[v]) {
                continue;
            }

            settled[v] = true;
            S.add(v);

            final double wv = totalWeight[v];

            for (final int e: g.getOutEdges(v)) {
                final IntSet endpoints = g.getVerticesAccessibleThrough(v, e);

                if (endpoints.size() != 1) {
                    throw new IllegalStateException("Only simple edges are allowed");
                }

                final int w = endpoints.iterator().nextInt();
                final double potentialWeight = wv + weights.apply(e);

                // Path discovery
                if (totalWeight[w] > potentialWeight) {
                    totalWeight[w] = potentialWeight;
                    Q.add(new Pair<>(w, potentialWeight));
                    sigma[w] = 0;
                    pred[w] = new IntArrayList(1);
                }

                // Path counting
                // If s=w and d(v)=0 and c(v,w)=0, it can happen that you end up here
                if (totalWeight[w] == potentialWeight && w != s) {
                    sigma[w] += sigma[v];
                    pred[w].add(v);
                }
            }
        }

        return new Pair<>(new Pair<>(pred, S), sigma);
    }

    public SparseFunction<Integer, Double> computeParallel() {
        final int n = g.getNumberOfVertices();

        final ProgressCounter counter = new ProgressCounter(n);
        counter.start();

        Int2DoubleMap betweenness = g.getVertices().parallelStream()
                .map(this::computePartial)
                .map(counter.relayCount())
                .reduce((a, b) -> {
                    final Int2DoubleMap sum = new Int2DoubleOpenHashMap(n);
                    sum.defaultReturnValue(0.0);

                    for (final int i: a.keySet()) {
                        sum.put(i, sum.get(i) + a.get(i));
                    }

                    for (final int i: b.keySet()) {
                        sum.put(i, sum.get(i) + b.get(i));
                    }

                    return sum;
                }).get();

        return new MapSparseFunction<>(betweenness);
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
                d.put(v, d.get(v) + c);
            }

            if (w != s) {
                betweenness.put(w, betweenness.get(w) + d.get(w));
            }
        }
    }
}
