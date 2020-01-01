package highways.algo.CH;

import highways.GraphWithWeights;
import highways.utils.*;
import it.unimi.dsi.fastutil.ints.*;

import java.util.*;
import java.util.stream.Collectors;

public class Preprocessing {
    public static int preprocessED(final GraphWithWeights<Double> g) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final int[] ed = new int[n];

        System.out.println("Compute initial ED");

        // Compute initial edge difference
        for (int v = 0; v < n; v++) {
            ed[v] = computeEdgeDifference(wg, v);
        }

        // Queue for next contraction
        PriorityQueue<Integer> queue = new PriorityQueue<>(Comparator.comparingInt(v -> ed[v]));

        for (int v = 0; v < n; v++) {
            queue.add(v);
        }

        final int REQUEUE_SIZE = 1_000;
        int untilRequeue = REQUEUE_SIZE;

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        System.out.println("Start contracting");

        while (!queue.isEmpty()) {
            final int v = queue.poll();

            for (final Edge sc: getShortcuts(wg, v)) {
                // Add shortcut (u,v,w) to graph
                final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                newWeights.put(e, sc.weight);
                numShortcuts++;
            }

            final IntSet neighbors = wg.graph.getNeighbours(v);

            wg.graph.removeVertex(v);

            // Update edge differences of all neighbors of v
            for (int w: neighbors) {
                untilRequeue--;
                ed[w] = computeEdgeDifference(wg, w);
            }



            // Only update queue irregularly
            // FIXME: changes the number of shortcuts generated
            if (untilRequeue < 0) {
                untilRequeue = REQUEUE_SIZE;

                PriorityQueue<Integer> newQueue = new PriorityQueue<>(Comparator.comparingInt(w -> ed[w]));
                newQueue.addAll(queue);

                queue = newQueue;
            }

            pc.count();
        }

        return numShortcuts;
    }

    public static int preprocessRandom(final GraphWithWeights<Double> g) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final IntArrayList queue = new IntArrayList(n);

        for (final int v: wg.graph.getVertices()) {
            queue.add(v);
        }

        Collections.shuffle(queue);

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        for (int k = 0; k < n; k++) {
            final int v = queue.getInt(k);

            for (final Edge sc: getShortcuts(wg, v)) {
                // Add shortcut (u,v,w) to graph
                final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);
                newWeights.put(e, sc.weight);

                numShortcuts++;
            }

            wg.graph.removeVertex(v);

            pc.count();
        }

        return numShortcuts;
    }

    public static int preprocessPriority(final GraphWithWeights<Double> g, final SparseFunction<Integer, Double> priority) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final double[] vertexWeights = new double[n];

        for (final int v: g.graph.getVertices()) {
            vertexWeights[v] = g.graph.getEdgesIncidentTo(v).stream().mapToDouble(priority::apply).max().getAsDouble();
        }

        PriorityQueue<Integer> queue = new PriorityQueue<>(Comparator.comparingDouble(v -> vertexWeights[v]));

        for (int v = 0; v < n; v++) {
            queue.add(v);
        }

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        System.out.println("Start contracting");

        while (!queue.isEmpty()) {
            final int v = queue.poll();

            for (final Edge sc: getShortcuts(wg, v)) {
                // Add shortcut (u,v,w) to graph
                final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                newWeights.put(e, sc.weight);
                numShortcuts++;
            }

            wg.graph.removeVertex(v);
            pc.count();
        }

        return numShortcuts;
    }

    public static int preprocessSimpleRamps(final GraphWithWeights<Double> g) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final IntSet rampNodes = new IntOpenHashSet(n);
        final IntSet pathNodes = new IntOpenHashSet(n);

        // Determine if vertex is a highway ramp
        for (final int v: g.graph.getVertices()) {
            if (wg.graph.getEdgeDegree(v) > 2) {
                rampNodes.add(v);
            } else {
                pathNodes.add(v);
            }
        }

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        System.out.println("Start contracting");

        while (!rampNodes.isEmpty() || !pathNodes.isEmpty()) {
            while (!pathNodes.isEmpty()) {
                final int v = pathNodes.iterator().nextInt();
                pathNodes.remove(v);

                // Contract
                for (final Edge sc: getShortcuts(wg, v)) {
                    // Add shortcut (u,v,w) to graph
                    final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                    newWeights.put(e, sc.weight);
                    numShortcuts++;
                }

                final IntSet neighbors = wg.graph.getNeighbours(v);

                wg.graph.removeVertex(v);

                for (final int u: neighbors) {
                    if (u != v) {
                        final boolean isRampNow = wg.graph.getEdgeDegree(u) > 2;

                        if (rampNodes.contains(u) && !isRampNow) {
                            rampNodes.remove(u);
                            pathNodes.add(u);
                        } else if (isRampNow) {
                            rampNodes.add(u);
                            pathNodes.remove(u);
                        }
                    }
                }

                pc.count();
            }

            if (!rampNodes.isEmpty()) {
                final int v = rampNodes.iterator().nextInt();
                rampNodes.remove(v);

                // Contract
                for (final Edge sc: getShortcuts(wg, v)) {
                    // Add shortcut (u,v,w) to graph
                    final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                    newWeights.put(e, sc.weight);
                    numShortcuts++;
                }

                final IntSet neighbors = wg.graph.getNeighbours(v);

                wg.graph.removeVertex(v);

                for (final int u: neighbors) {
                    if (u != v) {
                        final boolean isRampNow = wg.graph.getEdgeDegree(u) > 2;

                        if (rampNodes.contains(u) && !isRampNow) {
                            rampNodes.remove(u);
                            pathNodes.add(u);
                        } else if (isRampNow) {
                            rampNodes.add(u);
                            pathNodes.remove(u);
                        }
                    }
                }

                pc.count();
            }
        }

        return numShortcuts;
    }

    public static int preprocessContinuousRamps(final GraphWithWeights<Double> g, final SparseFunction<Integer, Double> priority) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final boolean[] rampNode = new boolean[n];

        // Determine if vertex is a highway ramp
        for (final int v: g.graph.getVertices()) {
            rampNode[v] = wg.graph.getEdgeDegree(v) > 2;
        }

        final double[] vertexWeights = new double[n];

        for (final int v: g.graph.getVertices()) {
            vertexWeights[v] = g.graph.getEdgesIncidentTo(v).stream().mapToDouble(e -> priority.apply(e)).sum();
        }

        final Comparator<Integer> comparator = Comparator.comparingDouble(v -> vertexWeights[v]);

        final PriorityQueue<Integer> rampQueue = new PriorityQueue<>(comparator);
        final PriorityQueue<Integer> nonRampQueue = new PriorityQueue<>(comparator);

        for (int v = 0; v < n; v++) {
            if (rampNode[v]) {
                rampQueue.add(v);
            } else {
                nonRampQueue.add(v);
            }
        }

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        System.out.println("Start contracting");

        while (!rampQueue.isEmpty() || !nonRampQueue.isEmpty()) {
            final int v;
            if (!nonRampQueue.isEmpty()) {
                v = nonRampQueue.poll();
            } else {
                v = rampQueue.poll();
            }

            for (final Edge sc: getShortcuts(wg, v)) {
                // Add shortcut (u,v,w) to graph
                final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                newWeights.put(e, sc.weight);
                numShortcuts++;
            }

            final IntSet neighbors = wg.graph.getNeighbours(v);

            wg.graph.removeVertex(v);

            for (final int u: neighbors) {
                final boolean isRampNow = wg.graph.getEdgeDegree(u) > 2;

                if (rampNode[u] != isRampNow) {
                    if (isRampNow) {
                        rampQueue.add(u);
                        nonRampQueue.remove(u);
                    } else {
                        rampQueue.remove(u);
                        nonRampQueue.add(u);
                    }
                    rampNode[u] = isRampNow;
                }
            }

            pc.count();
        }

        return numShortcuts;
    }

    public static int preprocessContinuousRampsNonUpdating(final GraphWithWeights<Double> g, final SparseFunction<Integer, Double> priority) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final boolean[] rampNode = new boolean[n];

        // Determine if vertex is a highway ramp
        for (final int v: g.graph.getVertices()) {
            rampNode[v] = wg.graph.getEdgeDegree(v) > 2;
        }

        final double[] vertexWeights = new double[n];

        for (final int v: g.graph.getVertices()) {
            vertexWeights[v] = g.graph.getEdgesIncidentTo(v).stream().mapToDouble(e -> priority.apply(e)).sum();
        }

        final Comparator<Integer> comparator = (v,w) ->
                rampNode[v] == rampNode[w] ?
                        Double.compare(vertexWeights[v], vertexWeights[w]) :
                        Boolean.compare(rampNode[v], rampNode[w]);

        final PriorityQueue<Integer> queue = new PriorityQueue<>(comparator);

        for (int v = 0; v < n; v++) {
            queue.add(v);
        }

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        System.out.println("Start contracting");

        while (!queue.isEmpty()) {
            final int v = queue.poll();

            for (final Edge sc: getShortcuts(wg, v)) {
                // Add shortcut (u,v,w) to graph
                final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                newWeights.put(e, sc.weight);
                numShortcuts++;
            }

            wg.graph.removeVertex(v);
            pc.count();
        }

        return numShortcuts;
    }

    public static int preprocessHighwayRamps(final GraphWithWeights<Double> g, final SparseFunction<Integer, Double> highwayness, final int k) {
        int numShortcuts = 0;
        final int n = g.graph.getNumberOfVertices();

        // Graph is assumed to be simple at the beginning
        final GraphWithWeights<Double> wg = g.simplify().x;
        final Int2DoubleMap newWeights = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> p: wg.weights.sparsePair()) {
            newWeights.put(p.x.intValue(), p.y.doubleValue());
        }

        wg.weights = new MapSparseFunction<>(newWeights);

        final ArrayList<Pair<Integer, Double>> sortedHighwayness = new ArrayList<>();

        for (final Pair<Integer, Double> p: highwayness.sparsePair()) {
            sortedHighwayness.add(p);
        }
        sortedHighwayness.sort((p, q) -> -Double.compare(p.y, q.y));

        final IntSet highwayEdges = sortedHighwayness.stream().limit(k).map(p -> p.x).collect(Collectors.toCollection(IntOpenHashSet::new));
        final boolean[] rampNode = new boolean[n];

        // Determine if vertex is a highway ramp
        for (final int v: g.graph.getVertices()) {
            final IntSet edges = g.graph.getEdgesIncidentTo(v);
            final int[] he = edges.stream().mapToInt(Integer::intValue).filter(highwayEdges::contains).toArray();

            if (he.length > 0 && (edges.size() > 2 || edges.size() != he.length)) {
                // Vertex has incident highway edge and is not just connecting two, but join roads
                rampNode[v] = true;
            }
        }

        final int[] ed = new int[n];

        // Compute initial edge difference
        for (final int v: g.graph.getVertices()) {
            ed[v] = computeEdgeDifference(wg, v);
        }

        // Queue for next contraction
        PriorityQueue<Integer> queue = new PriorityQueue<>((v,w) -> rampNode[v] == rampNode[w] ? Integer.compare(ed[v], ed[w]) : Boolean.compare(rampNode[v], rampNode[w]));

        for (int v = 0; v < n; v++) {
            queue.add(v);
        }

        final int REQUEUE_SIZE = 1_000;
        int untilRequeue = REQUEUE_SIZE;

        final ProgressCounter pc = new ProgressCounter(n);
        pc.start();

        System.out.println("Start contracting");

        while (!queue.isEmpty()) {
            final int v = queue.poll();

            for (final Edge sc: getShortcuts(wg, v)) {
                // Add shortcut (u,v,w) to graph
                final int e = wg.graph.addSimpleEdge(sc.u, sc.v, true);

                newWeights.put(e, sc.weight);
                numShortcuts++;
            }

            final IntSet neighbors = wg.graph.getNeighbours(v);

            wg.graph.removeVertex(v);

            // Update edge differences of all neighbors of v
            for (int w: neighbors) {
                untilRequeue--;
                ed[w] = computeEdgeDifference(wg, w);
            }



            // Only update queue irregularly
            // FIXME: changes the number of shortcuts generated
            if (untilRequeue < 0) {
                untilRequeue = REQUEUE_SIZE;

                PriorityQueue<Integer> newQueue = new PriorityQueue<>(Comparator.comparingInt(w -> ed[w]));
                newQueue.addAll(queue);

                queue = newQueue;
            }

            pc.count();
        }

        return numShortcuts;
    }

    static Pair<Int2DoubleMap, Int2DoubleMap> neighborPairs(final GraphWithWeights<Double> g, final int v) {
        // Store neighbor distance in map
        // to account for multi-edges between two nodes (take the minimum)
        Int2DoubleMap inNeighbors = new Int2DoubleOpenHashMap();
        inNeighbors.defaultReturnValue(Double.POSITIVE_INFINITY);
        Int2DoubleMap outNeighbors = new Int2DoubleOpenHashMap();
        outNeighbors.defaultReturnValue(Double.POSITIVE_INFINITY);

        for (final int e: g.graph.getInEdges(v)) {
            final boolean directed = g.graph.isDirectedSimpleEdge(e);
            final int u;

            if (directed) {
                u = g.graph.getDirectedSimpleEdgeTail(e);
            } else {
                u = g.graph.getVerticesAccessibleThrough(v, e).iterator().nextInt();
            }

            inNeighbors.put(u, Math.min(g.weights.apply(e), inNeighbors.get(u)));
        }

        for (final int e: g.graph.getOutEdges(v)) {
            final boolean directed = g.graph.isDirectedSimpleEdge(e);
            final int w;

            if (directed) {
                w = g.graph.getDirectedSimpleEdgeHead(e);
            } else {
                w = g.graph.getVerticesAccessibleThrough(v, e).iterator().nextInt();
            }

            outNeighbors.put(w, Math.min(g.weights.apply(e), outNeighbors.get(w)));
        }

        return new Pair<>(inNeighbors, outNeighbors);
    }

    private static int computeEdgeDifference(final GraphWithWeights<Double> g, final int v) {
        int numShortcuts = 0;

        Pair<Int2DoubleMap, Int2DoubleMap> np = neighborPairs(g, v);

        for (final Int2DoubleMap.Entry e1: np.x.int2DoubleEntrySet()) {
            final int u = e1.getIntKey();

            for (final Int2DoubleMap.Entry e2: np.y.int2DoubleEntrySet()) {
                final int w = e2.getIntKey();

                if (u != w) {
                    // How long is (u,v),(v,w)
                    final double viaV = e1.getDoubleValue() + e2.getDoubleValue();

                    if (!hasBetterPath(g, u, w, v, viaV)) {
                        numShortcuts++;
                    }
                }
            }
        }

        return numShortcuts - g.graph.getOutEdgeDegree(v) - g.graph.getInEdgeDegree(v);
    }

    private static class Edge {
        int u;
        int v;
        double weight;

        Edge(final int u, final int v, final double weight) {
            this.u = u; this.v = v; this.weight = weight;
        }
    }

    private static Collection<Edge> getShortcuts(final GraphWithWeights<Double> g, final int v) {
        final List<Edge> shortcuts = new ArrayList<>();

        Pair<Int2DoubleMap, Int2DoubleMap> np = neighborPairs(g, v);

        for (final Int2DoubleMap.Entry e1: np.x.int2DoubleEntrySet()) {
            final int u = e1.getIntKey();

            for (final Int2DoubleMap.Entry e2: np.y.int2DoubleEntrySet()) {
                final int w = e2.getIntKey();

                if (u != w) {
                    // Dijkstra with limited distance viaV, from u to w omitting v
                    // TODO: omitting v means (u,v)->(v,x)->(x,w) is not considered either
                    // TODO: and causes an unnecessary shortcut
                    //watch.start("findBetterPath");

                    // How long is (u,v),(v,w)
                    final double viaV = e1.getDoubleValue() + e2.getDoubleValue();

                    if (!hasBetterPath(g, u, w, v, viaV)) {
                        shortcuts.add(new Edge(u, w, viaV));
                    }
                    //watch.stop("findBetterPath");
                }
            }
        }

        return shortcuts;
    }

    static boolean hasBetterPath(
            final GraphWithWeights<Double> g,
            final int s,
            final int t,
            final int v,
            final double limitedDistance
    ) {
        // Simple check for vertices with only one in-edge
        if (g.graph.getInEdgeDegree(t) == 1) {
            return false;
        }

        // Use maps instead of arrays to make data structures sparse
        final Int2DoubleMap distance = new Int2DoubleOpenHashMap();
        distance.defaultReturnValue(Double.POSITIVE_INFINITY);
        final IntSet settled = new IntOpenHashSet();
        final IntOpenHashSet isViaV = new IntOpenHashSet();

        PriorityQueue<Int2DoubleMap.Entry> queue = new PriorityQueue<>(Comparator.comparingDouble(Int2DoubleMap.Entry::getDoubleValue));

        distance.put(s, 0.0);
        queue.add(new Int2DoubleOpenHashMap.BasicEntry(s, 0.0));

        while (!queue.isEmpty()) {
            final int u = queue.poll().getIntKey();
            final double dU = distance.get(u);

            // t is reached
            if (u == t) {
                break;
            }

            if (dU > limitedDistance) {
                throw new IllegalStateException("Should never settle vertices beyond t");
            }

            if (settled.contains(u)) {
                continue;
            }
            settled.add(u);

            for (final int e: g.graph.getOutEdges(u)) {
                final IntSet endpoints = g.graph.getVerticesAccessibleThrough(u, e);

                if (endpoints.size() != 1) {
                    throw new IllegalStateException("Only simple edges are allowed");
                }

                final int w = endpoints.iterator().nextInt();
                final double dist = dU + g.weights.apply(e);

                if (distance.get(w) > dist) {
                    // Mark if w was reached via v
                    if (u == v || isViaV.contains(u)) {
                        isViaV.add(w);
                    } else {
                        isViaV.remove(w);
                    }

                    if (w == t && dist <= limitedDistance && !isViaV.contains(w)) {
                        return true;
                    }

                    distance.put(w, dist);
                    queue.add(new Int2DoubleOpenHashMap.BasicEntry(w, dist));
                }
            }
        }

        return !isViaV.contains(t);
    }
}
