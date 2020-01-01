package highways.algo.approximations;

import highways.GraphWithWeights;
import highways.utils.Pair;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class MultiPathEdges {
    public IntSet compute(final GraphWithWeights<Double> g, final IntSet sources, final IntSet targets) {
        //final ProgressCounter counter = new ProgressCounter(sources.size());
        //counter.start();

        final IntSet edges = sources.parallelStream()
                .map(s -> compute(g, s, targets))
                //.map(counter.relayCount())
                .reduce((a, b) -> {
                    final IntSet union = new IntOpenHashSet(a.size() + b.size());

                    union.addAll(a);
                    union.addAll(b);

                    return union;
                }).get();

        return edges;
    }

    public IntSet compute(final GraphWithWeights<Double> g, final int s, final IntSet targets) {
        final int n = g.graph.getNumberOfVertices();

        final boolean[] settled = new boolean[n];
        final double[] dist = new double[n];
        final IntArrayList[] pred = new IntArrayList[n];

        for (int i = 0; i < n; i++) {
            dist[i] = Double.POSITIVE_INFINITY;
            pred[i] = null;
        }

        Queue<Pair<Integer, Double>> Q = new PriorityQueue<>(Comparator.comparingDouble(o -> o.y));

        dist[s] = 0.0;

        Q.add(new Pair<>(s, dist[s]));

        int numTargets = targets.size();

        while (!Q.isEmpty()) {
            final int v = Q.remove().x;

            if (settled[v]) {
                continue;
            }
            settled[v] = true;

            if (targets.contains(v)) {
                if (--numTargets == 0) {
                    break;
                }
            }

            final double dV = dist[v];

            for (int e: g.graph.getOutEdges(v)) {
                final IntSet endpoints = g.graph.getVerticesAccessibleThrough(v, e);

                if (endpoints.size() != 1) {
                    throw new IllegalStateException("Only simple edges are allowed");
                }

                final int w = endpoints.iterator().nextInt();
                final double potentialDistance = dV + g.weights.apply(e);

                // Path discovery
                if (dist[w] > potentialDistance) {
                    dist[w] = potentialDistance;
                    Q.add(new Pair<>(w, potentialDistance));
                    pred[w] = new IntArrayList();
                    pred[w].add(v);
                } else if (dist[w] == potentialDistance && g.weights.apply(e) > 0.0) {
                    pred[w].add(v);
                }
            }
        }

        final IntSet edges = new IntOpenHashSet();

        for (final int t: targets) {
            exploreEdges(g, pred, t, edges);
        }

        return edges;
    }

    private void exploreEdges(final GraphWithWeights<Double> g, final IntArrayList[] pred, final int t, final IntSet edges) {
        int curr = t;

        while (pred[curr] != null && pred[curr].size() == 1) {
            final int u = pred[curr].getInt(0);
            edges.add(g.graph.getSomeEdgeConnecting(u, curr));
            curr = u;
        }

        if (pred[curr] != null) {
            for (final int u: pred[curr]) {
                edges.add(g.graph.getSomeEdgeConnecting(u, curr));
                exploreEdges(g, pred, u, edges);
            }
        }
    }
}
