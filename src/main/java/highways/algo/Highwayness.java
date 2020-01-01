package highways.algo;

import grph.Grph;
import highways.utils.MapSparseFunction;
import highways.utils.Pair;
import highways.utils.ProgressCounter;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Stack;

abstract public class Highwayness {
    final Grph g;
    final SparseFunction<Integer, Double> weights;
    final SparseFunction<Integer, Double> distance;

    public Highwayness(
            final Grph g,
            final SparseFunction<Integer, Double> weights,
            final SparseFunction<Integer, Double> distance
    ) {
        this.g = g;
        this.weights = weights;
        this.distance = distance;
    }

    abstract Int2DoubleMap computePartial(final int s);

    public SparseFunction<Integer, Double> computeParallel() {
        final int n = g.getNumberOfVertices();

        final ProgressCounter counter = new ProgressCounter(n);
        counter.start();

        Int2DoubleMap betweenness = g.getVertices().parallelStream()
                .map(this::computePartial)
                .map(counter.relayCount())
                .reduce((a, b) -> {
                    final Int2DoubleOpenHashMap sum = new Int2DoubleOpenHashMap(n);

                    for (final int i: a.keySet()) {
                        sum.addTo(i, a.get(i));
                    }

                    for (final int i: b.keySet()) {
                        sum.addTo(i, b.get(i));
                    }

                    return sum;
                }).get();

        return new MapSparseFunction<>(betweenness);
    }

    Pair<Pair<IntArrayList[], Stack<Integer>>, Pair<double[], double[]>> computeExploration(final int s) {
        final int n = g.getNumberOfVertices();

        // Initialization
        final boolean[] settled = new boolean[n];
        final double[] totalWeight = new double[n];
        final double[] totalDistance = new double[n];

        final IntArrayList[] pred = new IntArrayList[n];

        for (int i = 0; i < n; i++) {
            totalWeight[i] = Double.POSITIVE_INFINITY;
            totalDistance[i] = Double.POSITIVE_INFINITY;
            pred[i] = null;
        }

        Stack<Integer> S = new Stack<>();

        Queue<Pair<Integer, Double>> Q = new PriorityQueue<>(Comparator.comparingDouble(o -> o.y));

        totalWeight[s] = 0.0;
        totalDistance[s] = 0.0;

        Q.add(new Pair<>(s, totalWeight[s]));

        while (!Q.isEmpty()) {
            final int v = Q.remove().x;

            if (settled[v]) {
                continue;
            }

            settled[v] = true;
            S.add(v);

            final double wv = totalWeight[v];

            for (int e: g.getOutEdges(v)) {
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
                    pred[w] = new IntArrayList();
                    totalDistance[w] = totalDistance[v] + distance.apply(e);
                }

                if (totalWeight[w] == potentialWeight && w != s) {
                    pred[w].add(v);
                }
            }
        }

        return new Pair<>(new Pair<>(pred, S), new Pair<>(totalWeight, totalDistance));
    }
}
