package highways;

import highways.algo.CH.Preprocessing;
import highways.generators.SwitchGenerator;
import highways.loaders.EdgeFileLoader;
import highways.loaders.PostgresLoader;
import highways.tasks.*;
import highways.utils.GraphUtils;
import highways.utils.MultiStopWatch;
import highways.utils.Pair;
import highways.utils.SparseFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

public class PostgresTasks {
    public static void printHelp() {
        System.out.println("Usage:");
        printCommandHelp();
    }
    public static void printCommandHelp() {
        System.out.println("\tpostgres NETWORK_ID POSTGRES_URL POSTGRES_USER [WEIGHT_COLUMN (default: cost)] [ALGORITHM (default: edge_betweenness)]");
    }
    public static void main(String[] args) {
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

        final PostgresDataDriver driver = new PostgresDataDriver(url, user, password);

        switch (algorithm) {
            case "ncc":
                TaskPostgresNcc.run(driver, network, weightColumn);
                return;
            case "chrandom":
                TaskChRandom.run(driver, network, weightColumn);
                return;
            case "chsimpleramps":
                TaskChSimpleRamps.run(driver, network, weightColumn);
                return;
            case "ched":
                TaskChEd.run(driver, network, weightColumn);
                return;
            case "chhpr":
                TaskChHpr.run(driver, network, weightColumn);
                return;
            case "chhprnu":
                TaskChHprNu.run(driver, network, weightColumn);
                return;
            case "generate_random":
                TaskGenerateRandom.run(driver, network, weightColumn);
                return;
        }

        final Iterable<Pair<Integer, Double>> edgeIterable;

        if ("phd".equals(algorithm)) {
            final Pair<GraphWithWeights<Double>[], Mappings> p;
            try {
                final Connection c = driver.getConnection();
                p = PostgresLoader.load(c, network, new String[]{weightColumn, "km"});
                c.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return;
            }

            edgeIterable = GraphUtils.runTwoWeightAlgorithm(algorithm, p);
        } else {
            final Pair<GraphWithWeights<Double>, Mappings> p = driver.loadGraph(network, weightColumn);
            edgeIterable = GraphUtils.runAlgorithm(algorithm, p);
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
            final Connection c = driver.getConnection();
            PostgresLoader.storeEdgeWeights(c, tableName, edgeIterable);
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Done");
    }
}
