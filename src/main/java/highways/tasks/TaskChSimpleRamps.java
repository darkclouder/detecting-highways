package highways.tasks;

import highways.AbstractDataDriver;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.algo.CH.Preprocessing;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;

public class TaskChSimpleRamps {

    public static void run(
            final AbstractDataDriver driver,
            final String network,
            final String weightColumn
    ) {
        final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(
                driver.loadGraph(network, weightColumn)
        );
        final MultiStopWatch watch = new MultiStopWatch();

        watch.start("sr");
        System.out.println("Simple ramps: " + Preprocessing.preprocessSimpleRamps(simplified.x));
        watch.stop("sr");

        System.out.println(watch);
    }
}
