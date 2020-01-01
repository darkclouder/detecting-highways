package highways;

import grph.Grph;
import grph.VertexPair;
import highways.utils.DoubleArraySparseFunction;
import highways.utils.ExtendableDoubleArraySparseFunction;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import toools.collections.LucIntSets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class GraphWithWeights<W extends Number> {
    public final Grph graph;
    public SparseFunction<Integer, W> weights;

    public GraphWithWeights(final Grph g, SparseFunction<Integer, W> w) {
        graph = g;
        weights = w;
    }

    public void store(final String outFile) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))) {

            for (VertexPair pair: graph.getEdgePairs()) {
                for (int e: graph.getEdgesConnecting(pair.first, pair.second)) {
                    double weight = weights.apply(e).doubleValue();

                    bw.write(String.format(
                        Locale.ROOT,
                        "%d %d %d %.6f\n",
                        e, pair.first, pair.second, weight
                    ));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Pair<GraphWithWeights<Double>, IntList> simplify() {
        final ArrayList<Double> w = new ArrayList<>();
        final Set<Pair<Integer, Integer>> visitedPairs = new HashSet<>();
        final Grph g = new FixedGrph();
        final IntList edgeMapping = new IntArrayList();

        for (final int v: graph.getVertices()) {
            g.addVertex(v);
        }

        // Remove multiple edges and loops
        for (final int u: graph.getVertices()) {
            for (final int v: graph.getOutNeighbors(u)) {
                final Pair<Integer, Integer> pair = new Pair<>(u, v);

                if (u != v && !visitedPairs.contains(pair)) {
                    final IntSet edges = LucIntSets.intersection(graph.getOutEdges(u), graph.getInEdges(v));
                    //final IntSet edges = graph.getEdgesConnecting(u, v);
                    final Pair<Integer, Double> minEdge = edges.stream()
                            .map(e -> new Pair<>(e, weights.apply(e).doubleValue()))
                            .min(Comparator.comparing(p -> p.y))
                            .get();

                    final boolean directed = graph.isDirectedSimpleEdge(minEdge.x) || g.areVerticesAdjacent(v, u);

                    final int edge = g.addSimpleEdge(u, v, directed);
                    assert edge == w.size();

                    edgeMapping.add(minEdge.x.intValue());

                    w.add(minEdge.y);

                    visitedPairs.add(pair);
                    if (!directed) {
                        visitedPairs.add(pair.revert());
                    }
                }
            }
        }

        return new Pair<>(new GraphWithWeights<>(
                g,
                new DoubleArraySparseFunction(
                        w.stream().mapToDouble(Double::doubleValue).toArray(),
                        Double.POSITIVE_INFINITY
                )
        ), edgeMapping);
    }

    public Pair<GraphWithWeights<Double>, Mappings> simplify(final Mappings originalMappings) {
        final int[] originalReverseEdgeMapping = originalMappings.edgeReverseMapping();
        final long[] originalReverseNodeMapping = originalMappings.nodeReverseMapping();
        final Int2IntMap edgeMapping = new Int2IntOpenHashMap();
        final Long2IntMap nodeMapping = new Long2IntOpenHashMap(originalMappings.nodeMapping.size());

        ArrayList<Double> w = new ArrayList<>();
        Set<Pair<Integer, Integer>> visitedPairs = new HashSet<>();
        final Grph g = new FixedGrph();

        for (final int u: graph.getVertices()) {
            final long uM = originalReverseNodeMapping[u];
            final int up;

            if (nodeMapping.containsKey(uM)) {
                up = nodeMapping.get(uM);
            } else {
                up = g.addVertex();
                nodeMapping.put(uM, up);
            }

            for (final int v: graph.getOutNeighbors(u)) {
                final Pair<Integer, Integer> pair = new Pair<>(u, v);

                // Remove multiple edges and loops
                if (u != v && !visitedPairs.contains(pair)) {
                    // Node mapping
                    final long vM = originalReverseNodeMapping[v];

                    final int vp;

                    if (nodeMapping.containsKey(vM)) {
                        vp = nodeMapping.get(vM);
                    } else {
                        vp = g.addVertex();
                        nodeMapping.put(vM, vp);
                    }

                    final IntSet edges = LucIntSets.intersection(graph.getOutEdges(u), graph.getInEdges(v));
                    final Pair<Integer, Double> minEdge = edges.stream()
                            .map(e -> new Pair<>(e, weights.apply(e).doubleValue()))
                            .min(Comparator.comparing(p -> p.y))
                            .get();

                    if (!graph.isSimpleEdge(minEdge.x)) {
                        throw new IllegalArgumentException("Graph is not simple");
                    }

                    // This edge is directed if it already was directed before
                    // Or if an edge in the other direction was already added.
                    // That edge is shorter in the other direction, but not in this one.
                    // Therefore the edge in this direction will be directed.
                    final boolean directed = graph.isDirectedSimpleEdge(minEdge.x) || g.areVerticesAdjacent(vp, up);

                    final int edge = g.addSimpleEdge(up, vp, directed);
                    assert edge == w.size();

                    edgeMapping.put(originalReverseEdgeMapping[minEdge.x], edge);
                    w.add(minEdge.y);

                    visitedPairs.add(pair);
                    if (!directed) {
                        visitedPairs.add(pair.revert());
                    }
                }
            }
        }

        return new Pair<> (
            new GraphWithWeights<>(
                g,
                new DoubleArraySparseFunction(
                        w.stream().mapToDouble(Double::doubleValue).toArray(),
                        Double.POSITIVE_INFINITY
                )
            ),
            new Mappings(nodeMapping, edgeMapping)
        );
    }

    public static GraphWithWeights<Double> makeDirected(final GraphWithWeights<Double> g) {
        if (g.graph.getNumberOfHyperEdges() > 0) {
            throw new IllegalArgumentException("Graph may not have hyper-edges");
        }

        final Grph directedGraph = new FixedGrph();
        final double[] weights = new double[2 * g.graph.getNumberOfUndirectedEdges() + g.graph.getNumberOfDirectedEdges()];

        for (final int e: g.graph.getEdges()) {
            final boolean directed = g.graph.isDirectedSimpleEdge(e);

            if (directed) {
                final int ed = directedGraph.addDirectedSimpleEdge(
                        g.graph.getDirectedSimpleEdgeTail(e),
                        g.graph.getDirectedSimpleEdgeHead(e)
                );
                weights[ed] = g.weights.apply(e);
            } else {
                final IntSet edges = g.graph.getVerticesIncidentToEdge(e);

                if (edges.size() != 2) {
                    throw new IllegalArgumentException("Graph may not have loops");
                }

                final IntIterator it = edges.iterator();
                final int u = it.nextInt();
                final int v = it.nextInt();

                int ed = directedGraph.addDirectedSimpleEdge(u, v);
                weights[ed] = g.weights.apply(e);

                ed = directedGraph.addDirectedSimpleEdge(v, u);
                weights[ed] = g.weights.apply(e);
            }
        }

        System.out.println("done directing");

        return new GraphWithWeights<>(directedGraph, new DoubleArraySparseFunction(weights));
    }

    public static GraphWithWeights<Double> copySimple(final GraphWithWeights<Double> g) {
        final boolean directed = g.graph.isDirected();

        final Grph cg = new FixedGrph();

        ArrayList<Double> w = new ArrayList<>();

        for (VertexPair pair: g.graph.getEdgePairs()) {
            final int e = cg.addSimpleEdge(pair.first, pair.second, directed);
            assert e == w.size();

            w.add(g.weights.apply(g.graph.getSomeEdgeConnecting(pair.first, pair.second)));
        }

        return new GraphWithWeights<>(
                cg,
                new ExtendableDoubleArraySparseFunction(w, Double.POSITIVE_INFINITY)
        );
    }
}
