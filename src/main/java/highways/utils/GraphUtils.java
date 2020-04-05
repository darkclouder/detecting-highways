package highways.utils;

import grph.Grph;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.algo.EdgeBetweennessCentrality;
import highways.algo.HighwaynessLength;
import highways.algo.ProportionateHighwaynessDistance;
import highways.algo.approximations.HwdPartitions;
import highways.generators.TriangleWeightGenerator;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import toools.collections.primitive.LucIntSet;

public class GraphUtils {
    public static IntSet getNonLargestCC(final Grph graph) {
        final IntSet ccVertices = graph.getLargestConnectedComponent();
        final IntSet noCcVertices = new IntOpenHashSet();

        for (int v: graph.getVertices()) {
            if (!ccVertices.contains(v)) {
                noCcVertices.add(v);
            }
        }

        return noCcVertices;
    }

    public static Pair<GraphWithWeights<Double>, Mappings> preprocess(Pair<GraphWithWeights<Double>, Mappings> p) {
        GraphWithWeights<Double> g = p.x;

        System.out.println("n=" + g.graph.getNumberOfVertices());
        System.out.println("m=" + g.graph.getNumberOfEdges());
        System.out.println("m'=" + (2 * g.graph.getNumberOfUndirectedEdges() + g.graph.getNumberOfDirectedEdges()));
        System.out.println("cc=" + g.graph.getConnectedComponents().size());

        System.out.println("Remove non-cc");
        final IntSet nonLargestCC = GraphUtils.getNonLargestCC(g.graph);
        g.graph.removeVertices(nonLargestCC);

        System.out.println("Simplify");
        final Pair<GraphWithWeights<Double>, Mappings> simplified = g.simplify(p.y);

        System.out.println("n=" + simplified.x.graph.getNumberOfVertices());
        System.out.println("m=" + simplified.x.graph.getNumberOfEdges());
        System.out.println("m'=" + (2 * simplified.x.graph.getNumberOfUndirectedEdges() + simplified.x.graph.getNumberOfDirectedEdges()));
        System.out.println("cc=" + simplified.x.graph.getConnectedComponents().size());

        return simplified;
    }

    public static Iterable<Pair<Integer, Double>> runAlgorithm(final String algorithm, Pair<GraphWithWeights<Double>, Mappings> p) {
        final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);
        final SparseFunction<Integer, Double> result;

        final MultiStopWatch watch = new MultiStopWatch();

        watch.start("algorithm");

        System.out.println("Run algorithm " + algorithm);

        switch (algorithm) {
            case "edge_betweenness": {
                result = new EdgeBetweennessCentrality(simplified.x.graph, simplified.x.weights).computeParallel();
                break;
            }
            case "highwayness_length": {
                result = new HighwaynessLength(
                        simplified.x.graph,
                        simplified.x.weights
                ).computeParallel();
                break;
            }
            case "approx_ebtw_partitions": {
                result = new HwdPartitions(
                        simplified.x
                ).computeEbtwParallel();
                break;
            }
            case "approx_hpr_partitions": {
                result = new HwdPartitions(
                        simplified.x
                ).computeHprParallel();
                break;
            }
            case "approx_partitions_skeleton": {
                result = new HwdPartitions(
                        simplified.x
                ).computeSkeleton();
                break;
            }
            case "generate_triangle_weights": {
                result = TriangleWeightGenerator.generate(simplified.x).weights;
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown algorithm");
        }

        watch.stop("algorithm");
        System.out.println(watch.toString());

        final int[] reverseMapping = simplified.y.edgeReverseMapping();
        final LucIntSet edges = simplified.x.graph.getEdges();

        return () -> edges.stream()
                .filter(e -> result.apply(e) != null)
                .map(e -> new Pair<>(reverseMapping[e], result.apply(e)))
                .iterator();
    }

    public static Iterable<Pair<Integer, Double>> runTwoWeightAlgorithm(final String algorithm, final Pair<GraphWithWeights<Double>[], Mappings> gws) {
        if (gws.x.length != 2) {
            throw new IllegalArgumentException("Can only take weight/distance pairs");
        }

        if (gws.x[0].graph != gws.x[1].graph) {
            throw new IllegalArgumentException("Graphs are not the same instance");
        }

        GraphWithWeights<Double> g = gws.x[0];
        Pair<GraphWithWeights<Double>, Mappings> simplified = preprocess(new Pair<>(g, gws.y));

        // Re-map distance labels after simplification

        final double[] distanceRemap = new double[simplified.y.edgeMapping.size()];

        {
            // new index to id
            int[] newReverseEdgeMapping = simplified.y.edgeReverseMapping();
            // id to old index
            Int2IntMap oldEdgeMapping = gws.y.edgeMapping;

            SparseFunction<Integer, Double> distance = gws.x[1].weights;

            for (int idx = 0; idx < newReverseEdgeMapping.length; idx++) {
                final int id = newReverseEdgeMapping[idx];
                final int oldIdx = oldEdgeMapping.get(id);
                distanceRemap[idx] = distance.apply(oldIdx);
            }
        }

        final SparseFunction<Integer, Double> distance = new DoubleArraySparseFunction(
                distanceRemap,
                Double.POSITIVE_INFINITY
        );

        long startTime = System.currentTimeMillis();

        System.out.println("Run algorithm " + algorithm);

        final SparseFunction<Integer, Double> result;

        switch (algorithm) {
            case "phd": {
                result = new ProportionateHighwaynessDistance(
                        simplified.x.graph,
                        simplified.x.weights,
                        distance
                ).computeParallel();
                break;
            }
            default:
                throw new IllegalArgumentException("Unknown algorithm");
        }

        long stopTime = System.currentTimeMillis();
        System.out.println((double) (stopTime - startTime) / 1_000 + "s");

        if (result == null) {
            return null;
        }

        final int[] reverseMapping = simplified.y.edgeReverseMapping();
        final LucIntSet edges = simplified.x.graph.getEdges();

        return () -> edges.stream()
                .filter(e -> result.apply(e) != null)
                .map(e -> new Pair<>(reverseMapping[e], result.apply(e)))
                .iterator();
    }
}
