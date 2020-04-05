package highways;

import highways.algo.CH.Preprocessing;
import highways.generators.SwitchGenerator;
import highways.loaders.EdgeFileLoader;
import highways.tasks.*;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;
import highways.utils.SparseFunction;

import java.io.File;

public class FileTasks {
    public static void printHelp() {
        System.out.println("Usage:");
        printCommandHelp();
    }
    public static void printCommandHelp() {
        System.out.println("\tfile FILE_PATH [WEIGHT (default: cost)] [DIRECTED (default: yes)] [ALGORITHM (default: edge_betweenness)] [META_FILE]");
    }

    public static void main(String[] args) {
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

        final FileDataDriver driver = new FileDataDriver(filePath, metaFile, directed);

        switch (algorithm) {
            case "ched":
                TaskChEd.run(driver, network, weightSelection);
                return;
            case "chrandom":
                TaskChRandom.run(driver, network, weightSelection);
                return;
            case "chsimpleramps":
                TaskChSimpleRamps.run(driver, network, weightSelection);
                return;
            case "chhpr":
                TaskChHpr.run(driver, network, weightSelection);
                return;
            case "chhprnu":
                TaskChHprNu.run(driver, network, weightSelection);
                return;
            case "generate_random":
                TaskGenerateRandom.run(driver, network, weightSelection);
                return;
        }

        final Pair<GraphWithWeights<Double>, Mappings> p = driver.loadGraph(network, weightSelection);
        final Iterable<Pair<Integer, Double>> edgeIterable = GraphUtils.runAlgorithm(algorithm, p);

        final String tableName = network + "_" + algorithm + "_" + weightSelection;
        final String outFile = "results/" + tableName + ".wedge";

        System.out.println("To file " + outFile);
        EdgeFileLoader.storeEdgeWeights(outFile, edgeIterable);

        System.out.println("Done");
    }

    public static Pair<GraphWithWeights<Double>, Mappings> loadGraph(
            final String weightSelection,
            final String filePath,
            final boolean directed
    ) {
        switch (weightSelection) {
            case "cost":
                return EdgeFileLoader.load(filePath, directed);
            case "uniform":
                return EdgeFileLoader.loadUniform(filePath, directed);
            default:
                throw new IllegalArgumentException("Unknown weight type");
        }
    }
}
