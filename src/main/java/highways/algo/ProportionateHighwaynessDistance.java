package highways.algo;

import grph.Grph;
import highways.utils.DoubleArraySparseFunction;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Stack;

public class ProportionateHighwaynessDistance extends Highwayness {
    public ProportionateHighwaynessDistance(
            final Grph g,
            final SparseFunction<Integer, Double> weights,
            final SparseFunction<Integer, Double> distance
    ) {
        super(g, weights, distance);
    }

    Int2DoubleMap computePartial(final int s) {
        final int n = g.getNumberOfVertices();

        final Int2DoubleOpenHashMap edgeHighwayness = new Int2DoubleOpenHashMap();
        edgeHighwayness.defaultReturnValue(0.0);

        // Get paths starting at s
        final Pair<Pair<IntArrayList[], Stack<Integer>>, Pair<double[],  double[]>> result = computeExploration(s);
        final IntArrayList[] pred = result.x.x;
        final Stack<Integer> S = result.x.y;
        final double[] totalDistance = result.y.y;

        // Start exploration of SP tree from leaves
        // and sum up all the 1/d(s,v) up on the way to s

        // Edges don't have to be summed up for all targets but the sums are aggregated over the way
        // to the source and then the fraction of the edge length (distance) to aggregated length is taken
        final double[] sigma = new double[n];

        // Go through all vertices in SP tree from last settled to first settled
        // That assures that a vertex is not picked before its sigma value is fully computed.
        while (!S.empty()) {
            final int w = S.pop();

            if (w != s) {
                // Get predecessor of a node

                for (final int v: pred[w]) {
                    // There is an edge (v,w)
                    final IntSet edges = g.getEdgesConnecting(v, w);

                    if (edges.size() != 1) {
                        throw new IllegalStateException("Does not allow multi-edges");
                    }

                    final int e = edges.iterator().nextInt();

                    final double sig = sigma[w] + 1.0 / totalDistance[w];

                    sigma[v] += sig;

                    // Here we take distance(e) into account
                    edgeHighwayness.put(e, sig * distance.apply(e));
                }
            }
        }

        return edgeHighwayness;
    }
}
