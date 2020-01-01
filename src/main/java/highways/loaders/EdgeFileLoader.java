package highways.loaders;

import grph.Grph;
import highways.FixedGrph;
import highways.GraphWithWeights;
import highways.Mappings;
import highways.utils.DoubleArraySparseFunction;
import highways.utils.MapSparseFunction;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.*;
import java.util.ArrayList;
import java.util.Locale;

public class EdgeFileLoader {
    public static Pair<GraphWithWeights<Double>, Mappings> load(final String edgeListFile, final boolean directed) {
        final Grph g = new FixedGrph();
        final Long2IntMap nodeMap = new Long2IntOpenHashMap();
        final Int2IntMap edgeMap = new Int2IntOpenHashMap();

        ArrayList<Double> w = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(edgeListFile))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] columns = line.split(" ");

                final int edgeId = Integer.parseInt(columns[0]);
                final int sourceId = Integer.parseInt(columns[1]);
                final int targetId = Integer.parseInt(columns[2]);
                final double distance = Double.parseDouble(columns[3]);

                if (!nodeMap.containsKey(sourceId)) {
                    nodeMap.put(sourceId, g.addVertex());
                }

                if (!nodeMap.containsKey(targetId)) {
                    nodeMap.put(targetId, g.addVertex());
                }

                final int e = g.addSimpleEdge(nodeMap.get(sourceId), nodeMap.get(targetId), directed);
                edgeMap.put(edgeId, e);

                assert e == w.size();

                w.add(distance);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Pair<>(
                new GraphWithWeights<>(
                        g,
                        new DoubleArraySparseFunction(
                                w.stream().mapToDouble(Double::doubleValue).toArray(),
                                Double.POSITIVE_INFINITY
                        )
                ),
                new Mappings(nodeMap, edgeMap)
        );
    }

    public static Pair<GraphWithWeights<Double>, Mappings> loadUniform(final String edgeListFile, final boolean directed) {
        final Grph g = new FixedGrph();
        final Long2IntMap nodeMap = new Long2IntOpenHashMap();
        final Int2IntMap edgeMap = new Int2IntOpenHashMap();

        ArrayList<Double> w = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(edgeListFile))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] columns = line.split(" ");

                final int edgeId = Integer.parseInt(columns[0]);
                final int sourceId = Integer.parseInt(columns[1]);
                final int targetId = Integer.parseInt(columns[2]);

                if (!nodeMap.containsKey(sourceId)) {
                    nodeMap.put(sourceId, g.addVertex());
                }

                if (!nodeMap.containsKey(targetId)) {
                    nodeMap.put(targetId, g.addVertex());
                }

                final int e = g.addSimpleEdge(nodeMap.get(sourceId), nodeMap.get(targetId), directed);
                edgeMap.put(edgeId, e);

                assert e == w.size();

                w.add(1.0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Pair<>(
                new GraphWithWeights<>(
                        g,
                        new DoubleArraySparseFunction(
                                w.stream().mapToDouble(Double::doubleValue).toArray(),
                                Double.POSITIVE_INFINITY
                        )
                ),
                new Mappings(nodeMap, edgeMap)
        );
    }

    public static SparseFunction<Integer, Double> loadEdgeWeights(final String edgeListFile, final Mappings mappings, final double defaultValue) {
        final Int2DoubleMap w = new Int2DoubleOpenHashMap();

        try (BufferedReader br = new BufferedReader(new FileReader(edgeListFile))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] columns = line.split(" ");

                final int e = mappings.edgeMapping.get(Integer.parseInt(columns[0]));
                final double weight = Double.parseDouble(columns[1]);

                w.put(e, weight);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new MapSparseFunction<>(w, defaultValue);
    }

    public static void storeEdgeWeights(
            final String edgeListFile,
            final Iterable<Pair<Integer, Double>> edges
    ) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(edgeListFile))) {

            for (final Pair<Integer, Double> edge: edges) {
                bw.write(String.format(
                        Locale.ROOT,
                        "%d %f\n",
                        edge.x,
                        edge.y
                ));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void storeDirectedGraph(
            final String edgeListFile,
            final GraphWithWeights<Double> g
    ) {
        if (!g.graph.isDirected()) {
            throw new IllegalArgumentException("Graph is not directed");
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(edgeListFile))) {

            for (final int e : g.graph.getEdges()) {
                bw.write(String.format(
                        Locale.ROOT,
                        "%d %d %d %f\n",
                        e,
                        g.graph.getDirectedSimpleEdgeTail(e),
                        g.graph.getDirectedSimpleEdgeHead(e),
                        g.weights.apply(e)
                ));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
