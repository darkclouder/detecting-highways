package highways;

import highways.loaders.PostgresLoader;
import highways.utils.GraphUtils;
import highways.utils.Pair;
import highways.utils.SparseFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresDataDriver extends AbstractDataDriver {
    private String url;
    private String user;
    private String password;

    public PostgresDataDriver(
            final String url,
            final String user,
            final String password
    ) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public Pair<GraphWithWeights<Double>, Mappings> loadGraph(String network, String weightColumn) {
        final Pair<GraphWithWeights<Double>, Mappings> p;

        try {
            final Connection c = getConnection();
            p = PostgresLoader.load(c, network, weightColumn);
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        return p;
    }

    public Pair<Pair<GraphWithWeights<Double>, Mappings>, SparseFunction<Integer, Double>> loadGraphWithPriority(
            final String network,
            final String weightColumn,
            final String priorityTable
    ) {
        final Pair<GraphWithWeights<Double>, Mappings> simplified;
        final SparseFunction<Integer, Double> priority;

        try {
            final Connection c = getConnection();
            final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
            simplified = GraphUtils.preprocess(p);
            priority = PostgresLoader.getEdgeWeights(c, priorityTable, simplified.y, 0.0);

            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        return new Pair<>(simplified, priority);
    }
}
