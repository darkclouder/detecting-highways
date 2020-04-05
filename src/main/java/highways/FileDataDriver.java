package highways;

import highways.loaders.EdgeFileLoader;
import highways.utils.GraphUtils;
import highways.utils.Pair;
import highways.utils.SparseFunction;

public class FileDataDriver extends AbstractDataDriver {
    private String filePath;
    private String metaFile;
    private boolean directed;

    public FileDataDriver(
            final String filePath,
            final String metaFile,
            final boolean directed
    ) {
        this.filePath = filePath;
        this.metaFile = metaFile;
        this.directed = directed;
    }

    @Override
    public Pair<GraphWithWeights<Double>, Mappings> loadGraph(String network, String weightColumn) {
        switch (weightColumn) {
            case "cost":
                return EdgeFileLoader.load(filePath, directed);
            case "uniform":
                return EdgeFileLoader.loadUniform(filePath, directed);
            default:
                throw new IllegalArgumentException("Unknown weight type");
        }
    }

    public Pair<Pair<GraphWithWeights<Double>, Mappings>, SparseFunction<Integer, Double>> loadGraphWithPriority(
            final String network,
            final String weightColumn,
            final String priorityTable
    ) {
        final Pair<GraphWithWeights<Double>, Mappings> p = loadGraph(network, weightColumn);
        final Pair<GraphWithWeights<Double>, Mappings> simplified = GraphUtils.preprocess(p);
        final SparseFunction<Integer, Double> priority = EdgeFileLoader.loadEdgeWeights(
                metaFile,
                simplified.y,
                0.0
        );
        return new Pair<>(simplified, priority);
    }
}
