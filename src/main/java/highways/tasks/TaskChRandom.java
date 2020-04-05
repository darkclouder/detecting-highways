package highways.tasks;

import highways.AbstractDataDriver;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.algo.CH.Preprocessing;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;

public class TaskChRandom {

    public static void run(
            final AbstractDataDriver driver,
            final String network,
            final String weightColumn
    ) {
        final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(
                driver.loadGraph(network, weightColumn)
        );
        final MultiStopWatch watch = new MultiStopWatch();

        watch.start("rnd");
        System.out.println("Random: " + Preprocessing.preprocessRandom(simplified.x));
        watch.stop("rnd");

        System.out.println(watch);
    }
}
