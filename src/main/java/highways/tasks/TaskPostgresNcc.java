package highways.tasks;

import highways.GraphWithWeights;
import highways.Mappings;
import highways.PostgresDataDriver;
import highways.loaders.PostgresLoader;
import highways.utils.GraphUtils;
import highways.utils.Pair;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.sql.Connection;
import java.sql.SQLException;

public class TaskPostgresNcc {
    public static void run(
            final PostgresDataDriver driver,
            final String network,
            final String weightColumn
    ) {
        final Pair<GraphWithWeights<Double>, Mappings> p = driver.loadGraph(network, weightColumn);

        System.out.println("n=" + p.x.graph.getNumberOfVertices());
        System.out.println("m=" + p.x.graph.getNumberOfEdges());
        System.out.println("cc=" + p.x.graph.getConnectedComponents().size());

        final long[] reverseMapping = p.y.nodeReverseMapping();
        final IntSet noCcVertices = GraphUtils.getNonLargestCC(p.x.graph);

        System.out.println(p.x.graph.getLargestConnectedComponent().size());

        final String tableName = network + "_non_cc";

        try {
            final Connection c = driver.getConnection();
            PostgresLoader.storeNodeList(
                    c,
                    tableName,
                    () -> noCcVertices.stream().mapToLong(v -> reverseMapping[v]).iterator()
            );
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
