package highways.loaders;

import grph.Grph;
import highways.FixedGrph;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.utils.DoubleArraySparseFunction;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.sql.*;
import java.util.ArrayList;

public class PostgresLoader {
    public static Pair<GraphWithWeights<Double>, Mappings> load(
            final Connection connection,
            final String table,
            final String weightColumn
    ) {
        final Pair<GraphWithWeights<Double>[], Mappings> p = load(connection, table, new String[] {weightColumn});
        return new Pair<>(p.x[0], p.y);
    }

    public static Pair<GraphWithWeights<Double>[], Mappings> load(
            final Connection connection,
            final String table,
            final String[] weightColumns
    ) {
        final Grph g = new FixedGrph();
        final ArrayList<Double>[] w = new ArrayList[weightColumns.length];

        final Long2IntMap nodeMapping = new Long2IntOpenHashMap();
        final Int2IntMap edgeMapping = new Int2IntOpenHashMap();

        try {
            final Statement stmt = connection.createStatement();

            System.out.println(String.format("Querying %s", table));

            StringBuilder selectQuery = new StringBuilder();

            selectQuery.append("SELECT id, osm_source_id, osm_target_id, reverse_cost>=1000000 AS one_way");

            for (int i = 0; i < weightColumns.length; i++) {
                selectQuery.append(", ");
                selectQuery.append(weightColumns[i]);
                selectQuery.append(" AS w_");
                selectQuery.append(i);
                w[i] = new ArrayList<>();
            }

            selectQuery.append(" FROM ");
            selectQuery.append(table);

            final ResultSet rs = stmt.executeQuery(selectQuery.toString());

            System.out.println(String.format("Retrieving %s", table));

            final int step = 10_000;
            int c = 0;

            while (rs.next()) {
                if (c % step == 0) {
                    System.out.print('*');
                }
                c++;

                final int edge = rs.getInt(1);
                final long source = rs.getLong(2);
                final long target = rs.getLong(3);
                final boolean directed = rs.getBoolean(4);

                if (!nodeMapping.containsKey(source)) {
                    nodeMapping.put(source, nodeMapping.size());
                }
                if (!nodeMapping.containsKey(target)) {
                    nodeMapping.put(target, nodeMapping.size());
                }

                final int sourceId = nodeMapping.get(source);
                final int targetId = nodeMapping.get(target);

                edgeMapping.put(edge, g.addSimpleEdge(sourceId, targetId, directed));

                for (int i = 0; i < weightColumns.length; i++) {
                    w[i].add(rs.getDouble(5 + i));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println();

        GraphWithWeights<Double>[] gws = new GraphWithWeights[weightColumns.length];

        for (int i = 0; i < weightColumns.length; i++) {
            gws[i] = new GraphWithWeights<>(
                    g,
                    new DoubleArraySparseFunction(
                            w[i].stream().mapToDouble(Double::doubleValue).toArray(),
                            Double.POSITIVE_INFINITY
                    )
            );
        }

        return new Pair<>(gws, new Mappings(nodeMapping, edgeMapping));
    }

    public static void storeNodeList(final Connection connection,
                                     final String table,
                                     Iterable<Long> nodes) {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(String.format("DROP TABLE IF EXISTS %s", table));
            stmt.execute(String.format("CREATE TABLE %s (osm_node_id BIGINT NOT NULL UNIQUE)", table));

            PreparedStatement stmt2 = connection.prepareStatement(
                    String.format("INSERT INTO %s (osm_node_id) VALUES (?)", table)
            );

            for (long node: nodes) {
                stmt2.setLong(1, node);
                stmt2.addBatch();
            }

            stmt2.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void storeEdgeWeights(
            final Connection connection,
            final String table,
            final Iterable<Pair<Integer, Double>> edges
    ) {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(String.format("DROP TABLE IF EXISTS %s", table));
            stmt.execute(String.format(
                    "CREATE TABLE %s (edge_id BIGINT NOT NULL UNIQUE, weight REAL)"
                    , table)
            );

            PreparedStatement stmt2 = connection.prepareStatement(
                    String.format("INSERT INTO %s (edge_id, weight) VALUES (?,?)", table)
            );

            for (final Pair<Integer, Double> edge: edges) {
                stmt2.setInt(1, edge.x);
                stmt2.setDouble(2, edge.y);
                stmt2.addBatch();
            }

            stmt2.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static SparseFunction<Integer, Double> getEdgeWeights(
            final Connection connection,
            final String table,
            final Mappings mappings,
            final double defaultValue
    ) throws SQLException {
        final Statement stmt = connection.createStatement();
        final ResultSet rs = stmt.executeQuery(String.format(
                "SELECT edge_id, weight FROM %s",
                table
        ));

        final double[] weights = new double[mappings.edgeMapping.size()];

        while (rs.next()) {
            final int e = mappings.edgeMapping.get(rs.getInt(1));
            weights[e] = rs.getDouble(2);
        }

        return new DoubleArraySparseFunction(weights, defaultValue);
    }
}
