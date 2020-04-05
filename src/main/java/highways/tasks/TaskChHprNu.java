package highways.tasks;

import highways.AbstractDataDriver;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.algo.CH.Preprocessing;
import highways.loaders.PostgresLoader;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;
import highways.utils.SparseFunction;

import java.sql.DriverManager;
import java.sql.SQLException;

public class TaskChHprNu {
    public static void run(
            final AbstractDataDriver driver,
            final String network,
            final String weightColumn
    ) {
        final Pair<Pair<GraphWithWeights<Double>, Mappings>, SparseFunction<Integer, Double>> simplifiedWithPriority
                = driver.loadGraphWithPriority(
                network,
                weightColumn,
                network + "_highwayness_length_cost"
        );

        final Pair<GraphWithWeights<Double>, Mappings> simplified = simplifiedWithPriority.x;
        final SparseFunction<Integer, Double> priority = simplifiedWithPriority.y;

        final MultiStopWatch watch = new MultiStopWatch();

        watch.start("hpr");
        System.out.println("Cont. Ramps: " + Preprocessing.preprocessContinuousRampsNonUpdating(simplified.x, priority));
        watch.stop("hpr");

        System.out.println(watch);
    }
}
