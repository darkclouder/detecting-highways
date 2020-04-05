package highways;

import highways.loaders.PostgresLoader;
import highways.utils.GraphUtils;
import highways.utils.Pair;
import highways.utils.SparseFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

abstract class DataDriver {
    public abstract Pair<GraphWithWeights<Double>, Mappings> loadGraph(
            final String network,
            final String weightColumn
    );

    public abstract SparseFunction<Integer, Double> loadEdgeWeights(
            final Pair<GraphWithWeights<Double>, Mappings> simplified,
            final String field
    );
}


