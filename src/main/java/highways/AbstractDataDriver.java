package highways;

import highways.loaders.PostgresLoader;
import highways.utils.GraphUtils;
import highways.utils.Pair;
import highways.utils.SparseFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

abstract public class AbstractDataDriver {
    abstract public Pair<GraphWithWeights<Double>, Mappings> loadGraph(String network, String weightColumn);

    abstract public Pair<Pair<GraphWithWeights<Double>, Mappings>, SparseFunction<Integer, Double>> loadGraphWithPriority(
            final String network,
            final String weightColumn,
            final String priorityTable
    );
}
