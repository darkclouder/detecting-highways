package highways;

import highways.algo.CH.Preprocessing;
import highways.generators.SwitchGenerator;
import highways.loaders.EdgeFileLoader;
import highways.loaders.PostgresLoader;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Connection c;

        if (args.length == 0) {
            printHelp();
            return;
        }

        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        switch (args[0]) {
            case "file":
                file(subArgs);
                break;
            case "postgres":
                postgres(subArgs);
                break;
            default:
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\tfile FILE_PATH [WEIGHT (default: cost)] [DIRECTED (default: yes)] [ALGORITHM (default: edge_betweenness)] [META_FILE]");
        System.out.println("\tpostgres NETWORK_ID POSTGRES_URL POSTGRES_USER [WEIGHT_COLUMN (default: cost)] [ALGORITHM (default: edge_betweenness)]");
    }

    private static void postgres(String[] args) {
        if (args.length < 3) {
            printHelp();
            return;
        }

        final String network = args[0];
        final String url = args[1];
        final String user = args[2];
        final String weightColumn = (args.length > 3) ? args[3] : "cost";
        final String algorithm = (args.length > 4) ? args[4] : "edge_betweenness";

        final Scanner scan = new Scanner(System.in);

        String password = System.getenv("POSTGRES_PASSWORD");

        if (password == null) {
            System.out.println("Password: ");
            password = scan.next();
        }

        Connection c;
        final Iterable<Pair<Integer, Double>> edgeIterable;

        switch (algorithm) {
            case "ncc": {
                Pair<GraphWithWeights<Double>, Mappings> p;
                try {
                    c = DriverManager.getConnection(url, user, password);
                    p = PostgresLoader.load(c, network, weightColumn);
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                System.out.println("n=" + p.x.graph.getNumberOfVertices());
                System.out.println("m=" + p.x.graph.getNumberOfEdges());
                System.out.println("cc=" + p.x.graph.getConnectedComponents().size());

                final long[] reverseMapping = p.y.nodeReverseMapping();
                final IntSet noCcVertices = GraphUtils.getNonLargestCC(p.x.graph);

                System.out.println(p.x.graph.getLargestConnectedComponent().size());

                final String tableName = network + "_non_cc";

                try {
                    c = DriverManager.getConnection(url, user, password);
                    PostgresLoader.storeNodeList(
                            c,
                            tableName,
                            () -> noCcVertices.stream().mapToLong(v -> reverseMapping[v]).iterator()
                    );
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                return;
            }
            case "chrandom": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified;

                try {
                    c = DriverManager.getConnection(url, user, password);
                    final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
                    simplified = GraphUtils.preprocess(p);

                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("rnd");
                System.out.println("Random: " + Preprocessing.preprocessRandom(simplified.x));
                watch.stop("rnd");

                System.out.println(watch.toString());
                return;
            }
            case "chsimpleramps": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified;

                try {
                    c = DriverManager.getConnection(url, user, password);
                    final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
                    simplified = GraphUtils.preprocess(p);

                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("sr");
                System.out.println("Simple ramps: " + Preprocessing.preprocessSimpleRamps(simplified.x));
                watch.stop("sr");

                System.out.println(watch.toString());
                return;
            }
            case "ched": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified;

                try {
                    c = DriverManager.getConnection(url, user, password);
                    final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
                    simplified = GraphUtils.preprocess(p);

                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("ed");
                System.out.println("ED: " + Preprocessing.preprocessED(simplified.x));
                watch.stop("ed");

                System.out.println(watch.toString());
                return;
            }
            case "chhpr": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified;
                final SparseFunction<Integer, Double> priority;

                try {
                    c = DriverManager.getConnection(url, user, password);
                    final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
                    simplified = GraphUtils.preprocess(p);

                    priority = PostgresLoader.getEdgeWeights(c, network + "_highwayness_length_cost", simplified.y, 0.0);

                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("hpr");
                System.out.println("Cont. Ramps: " + Preprocessing.preprocessContinuousRamps(simplified.x, priority));
                watch.stop("hpr");

                System.out.println(watch.toString());
                return;
            }
            case "chhprnu": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified;
                final SparseFunction<Integer, Double> priority;

                try {
                    c = DriverManager.getConnection(url, user, password);
                    final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
                    simplified = GraphUtils.preprocess(p);

                    priority = PostgresLoader.getEdgeWeights(c, network + "_highwayness_length_cost", simplified.y, 0.0);

                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("hpr");
                System.out.println("Cont. Ramps: " + Preprocessing.preprocessContinuousRampsNonUpdating(simplified.x, priority));
                watch.stop("hpr");

                System.out.println(watch.toString());
                return;
            }
            case "generate_random": {
                final GraphWithWeights<Double> directed;

                try {
                    c = DriverManager.getConnection(url, user, password);
                    final Pair<GraphWithWeights<Double>, Mappings> p = PostgresLoader.load(c, network, weightColumn);
                    directed = GraphWithWeights.makeDirected(GraphUtils.preprocess(p).x);
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                System.out.println("Store directed original graph");

                EdgeFileLoader.storeDirectedGraph("results/" + network + ".txt", directed);

                for (int i = 0; i < 10; i++) {
                    System.out.println("Generate random graph");

                    final long seed = System.currentTimeMillis();
                    final GraphWithWeights<Double> randomGraph = SwitchGenerator.generate(directed, seed);

                    System.out.println("Store random graph");
                    EdgeFileLoader.storeDirectedGraph("results/" + network + "_rnd_" + seed + ".txt", randomGraph);
                }

                return;
            }
        }

        switch (algorithm) {
            case "phd": {
                Pair<GraphWithWeights<Double>[], Mappings> p;
                try {
                    c = DriverManager.getConnection(url, user, password);
                    p = PostgresLoader.load(c, network, new String[] { weightColumn, "km" });
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                edgeIterable = GraphUtils.runTwoWeightAlgorithm(algorithm, p);
                break;
            }
            default: {
                Pair<GraphWithWeights<Double>, Mappings> p;
                try {
                    c = DriverManager.getConnection(url, user, password);
                    p = PostgresLoader.load(c, network, weightColumn);
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                }

                edgeIterable = GraphUtils.runAlgorithm(algorithm, p);
            }
        }

        if (edgeIterable == null) {
            System.out.println("Exit without result");
            return;
        }

        System.out.println("Store results");

        final String tableName = network + "_" + algorithm + "_" + weightColumn;

        System.out.println("To file");
        EdgeFileLoader.storeEdgeWeights("results/" + tableName + ".wedge", edgeIterable);

        System.out.println("To DB");

        try {
            c = DriverManager.getConnection(url, user, password);
            PostgresLoader.storeEdgeWeights(c, tableName, edgeIterable);
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Done");
    }

    private static void file(String[] args) {
        if (args.length < 1) {
            printHelp();
            return;
        }

        final String filePath = args[0];
        final String weightSelection = (args.length > 1) ? args[1] : "cost";
        final boolean directed = args.length <= 2 || !args[2].equals("no");
        final String algorithm = (args.length > 3) ? args[3] : "edge_betweenness";
        final String metaFile = (args.length > 4) ? args[4] : "";

        final File f = new File(filePath);
        System.out.println("Input file name: " + f.getName());
        final String network = f.getName().replace(".", "-");

        Pair<GraphWithWeights<Double>, Mappings> p;

        switch (weightSelection) {
            case "cost":
                p = EdgeFileLoader.load(filePath, directed);
                break;
            case "uniform":
                p = EdgeFileLoader.loadUniform(filePath, directed);
                break;
            default:
                throw new IllegalArgumentException("Unknown weight type");
        }

        final Iterable<Pair<Integer, Double>> edgeIterable;

        switch (algorithm) {
            case "ched": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("ed");
                System.out.println("ED: " + Preprocessing.preprocessED(simplified.x));
                watch.stop("ed");

                System.out.println(watch.toString());
                return;
            }
            case "chsimpleramps": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);
                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("sr");
                System.out.println("Simple Ramps: " + Preprocessing.preprocessSimpleRamps(simplified.x));
                watch.stop("sr");

                System.out.println(watch.toString());
                return;
            }
            case "chrandom": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);
                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("rnd");
                System.out.println("Random: " + Preprocessing.preprocessRandom(simplified.x));
                watch.stop("rnd");

                System.out.println(watch.toString());
                return;
            }
            case "chhpr": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);
                final SparseFunction<Integer, Double> priority = EdgeFileLoader.loadEdgeWeights(metaFile, simplified.y, 0.0);

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("hpr");
                System.out.println("Cont. Ramps: " + Preprocessing.preprocessContinuousRamps(simplified.x, priority));
                watch.stop("hpr");

                System.out.println(watch.toString());
                return;
            }
            case "chhprnu": {
                final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);
                final SparseFunction<Integer, Double> priority = EdgeFileLoader.loadEdgeWeights(metaFile, simplified.y, 0.0);

                final MultiStopWatch watch = new MultiStopWatch();

                watch.start("hprnu");
                System.out.println("Cont. Ramps non-updating: " + Preprocessing.preprocessContinuousRampsNonUpdating(simplified.x, priority));
                watch.stop("hprnu");

                System.out.println(watch.toString());
                return;
            }
            case "generate_random": {
                final GraphWithWeights<Double> dg = GraphWithWeights.makeDirected(GraphUtils.preprocess(p).x);

                System.out.println("Store directed original graph");

                EdgeFileLoader.storeDirectedGraph("results/" + network + ".txt", dg);

                for (int i = 0; i < 10; i++) {
                    System.out.println("Generate random graph");

                    final long seed = System.currentTimeMillis();
                    final GraphWithWeights<Double> randomGraph = SwitchGenerator.generate(dg, seed);

                    System.out.println("Store random graph");
                    EdgeFileLoader.storeDirectedGraph("random/" + network + "_rnd_" + seed + ".txt", randomGraph);
                }

                return;
            }
            default: {
                edgeIterable = GraphUtils.runAlgorithm(algorithm, p);
            }
        }

        final String tableName = network + "_" + algorithm + "_" + weightSelection;
        final String outFile = "results/" + tableName + ".wedge";

        System.out.println("To file " + outFile);
        EdgeFileLoader.storeEdgeWeights(outFile, edgeIterable);

        System.out.println("Done");
    }
}
