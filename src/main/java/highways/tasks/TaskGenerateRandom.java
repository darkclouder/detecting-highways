package highways.tasks;

import highways.AbstractDataDriver;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.algo.CH.Preprocessing;
import highways.generators.SwitchGenerator;
import highways.loaders.EdgeFileLoader;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;

public class TaskGenerateRandom {
    public static void run(
            final AbstractDataDriver driver,
            final String network,
            final String weightColumn
    ) {
        final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(
                driver.loadGraph(network, weightColumn)
        );
        final GraphWithWeights<Double> directed = GraphWithWeights.makeDirected(simplified.x);

        System.out.println("Store directed original graph");

        EdgeFileLoader.storeDirectedGraph("results/" + network + ".txt", directed);

        for (int i = 0; i < 10; i++) {
            System.out.println("Generate random graph");

            final long seed = System.currentTimeMillis();
            final GraphWithWeights<Double> randomGraph = SwitchGenerator.generate(directed, seed);

            System.out.println("Store random graph");
            EdgeFileLoader.storeDirectedGraph("results/" + network + "_rnd_" + seed + ".txt", randomGraph);
        }
    }
}
