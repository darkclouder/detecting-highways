package highways.generators;

import grph.Grph;
import highways.FixedGrph;
import highways.GraphWithWeights;
import highways.utils.ProgressCounter;

import java.util.Random;

public class SwitchGenerator {
    public static GraphWithWeights<Double> generate(final GraphWithWeights<Double> original, final long seed) {
        if (!original.graph.isDirectedSimpleGraph()) {
            throw new IllegalArgumentException("Graph needs to be directed and simple");
        }

        // Base idea: https://arxiv.org/pdf/cond-mat/0312028.pdf (followup papers)
        // Take an edge and switch endpoints

        // Own contribution: Go through each edge exactly one and pick a second one to switch with my random
        // This way every edge gets a change of being switched (and if it is picked again, it's not switched)
        // With the result of the same edge reoccurring equally likely than any other configuration

        final Grph randomGraph = new FixedGrph();
        final Random random = new Random(seed);

        for (final int e: original.graph.getEdges()) {
            final int u = original.graph.getDirectedSimpleEdgeTail(e);
            final int v = original.graph.getDirectedSimpleEdgeHead(e);
            randomGraph.addDirectedSimpleEdge(u, e, v);
        }

        final ProgressCounter progress = new ProgressCounter(original.graph.getNumberOfEdges());
        progress.start();

        for (final int e1: original.graph.getEdges()) {
            progress.count();
            final int e2 = randomGraph.getEdges().pickRandomElement(random);

            // Do not switch the same edge
            if (e1 == e2) {
                continue;
            }

            // e1=(u,v)
            final int u = randomGraph.getDirectedSimpleEdgeTail(e1);
            final int v = randomGraph.getDirectedSimpleEdgeHead(e1);
            // e2=(x,y)
            final int x = randomGraph.getDirectedSimpleEdgeTail(e2);
            final int y = randomGraph.getDirectedSimpleEdgeHead(e2);

            // Do not create loops
            if (u == y || x == v) {
                continue;
            }

            // Do not switch if (u,y) or (x, v) already exists
            if (randomGraph.areVerticesAdjacent(u, y) || randomGraph.areVerticesAdjacent(x, v)) {
                continue;
            }

            if (randomGraph.containsEdge(e1)) {
                randomGraph.removeEdge(e1);
            }

            if (randomGraph.containsEdge(e2)) {
                randomGraph.removeEdge(e2);
            }

            randomGraph.addDirectedSimpleEdge(u, e1, y);
            randomGraph.addDirectedSimpleEdge(x, e2, v);
        }

        return new GraphWithWeights<>(randomGraph, original.weights);
    }
}
