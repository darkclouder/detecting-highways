package highways.generators;

import highways.GraphWithWeights;
import highways.utils.MapSparseFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

public class TriangleWeightGenerator {
    public static GraphWithWeights<Double> generate(final GraphWithWeights<Double> original) {
        if (!original.graph.isUndirectedSimpleGraph()) {
            throw new IllegalArgumentException("Graph must be undirected and simple");
        }

        final Int2DoubleOpenHashMap triangleCounts = new Int2DoubleOpenHashMap(original.graph.getNumberOfEdges());

        for (final int e: original.graph.getEdges()) {
            final IntSet vertices = original.graph.getVerticesIncidentToEdge(e);

            if (vertices.size() != 2) {
                throw new IllegalArgumentException("Graph must be simple");
            }

            final IntSet commonNeighbors = vertices.stream()
                    .map(original.graph::getNeighbours)
                    .reduce((n1, n2) -> {
                        n1.retainAll(n2);
                        return n1;
                    })
                    .get();

            triangleCounts.put(e, commonNeighbors.size());
        }

        return new GraphWithWeights<>(
                original.graph,
                new MapSparseFunction<>(triangleCounts)
        );
    }
}
