package highways.algo.approximations;

import highways.FixedGrph;
import highways.GraphWithWeights;
import highways.algo.EdgeBetweennessCentrality;
import highways.algo.HighwaynessLength;
import highways.utils.MapSparseFunction;
import highways.utils.Pair;
import highways.utils.SparseFunction;
import it.unimi.dsi.fastutil.ints.*;
import toools.collections.LucIntSets;

import java.util.*;

public class HwdPartitions {
    private final GraphWithWeights<Double> g;

    public HwdPartitions(
            final GraphWithWeights<Double> g
    ) {
        this.g = g;
    }

    public SparseFunction<Integer, Double> computeSkeleton() {
        final int n = g.graph.getNumberOfVertices();
        final int k = Math.round((float)Math.sqrt(n) + 1);

        System.out.println("Create partitions");
        final Pair<IntSet, IntSet>[] partitions = createPartitions(k);

        System.out.println("Create partition skeletons");

        final Map<Integer, Double> skeletonEdges = new HashMap<>();

        System.out.println("Get skeleton edges");

        final Pair<GraphWithWeights<Double>, Pair<Int2ObjectMap<IntArrayList>, IntArrayList>> skeleton = createSkeleton(partitions);


        for (final IntArrayList edges: skeleton.y.x.values()) {
            for (final int e: edges) {
                skeletonEdges.put(e, 1.0);
            }
        }


        return new MapSparseFunction<>(skeletonEdges);
    }

    public SparseFunction<Integer, Double> computeEbtwParallel() {
        final int n = g.graph.getNumberOfVertices();
        final int k = Math.round((float)Math.sqrt(n) + 1);

        System.out.println("Create partitions");
        final Pair<IntSet, IntSet>[] partitions = createPartitions(k);

        System.out.println("Create partition skeletons");
        final Pair<GraphWithWeights<Double>, Pair<Int2ObjectMap<IntArrayList>, IntArrayList>> skeleton = createSkeleton(partitions);

        // TODO
        // We assume that k/b of all paths go through one border node
        // Therefore we compute Highwayness for all border nodes and factorize the length with 1/k

        final SparseFunction<Integer, Double> ebtw = new EdgeBetweennessCentrality(skeleton.x.graph, skeleton.x.weights).computeParallel();

        final Int2ObjectMap<IntArrayList> contractions = skeleton.y.x;
        final Int2DoubleMap fullEbtw = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> pair: ebtw.sparsePair()) {
            final double weight = pair.y;

            for (final int e: contractions.get(pair.x.intValue())) {
                fullEbtw.put(e, weight);
            }
        }

        return new MapSparseFunction<>(fullEbtw);
    }

    public SparseFunction<Integer, Double> computeHprParallel() {
        final int n = g.graph.getNumberOfVertices();
        final int k = Math.round((float)Math.sqrt(n) + 1);

        System.out.println("Create partitions");
        final Pair<IntSet, IntSet>[] partitions = createPartitions(k);

        System.out.println("Create partition skeletons");
        final Pair<GraphWithWeights<Double>, Pair<Int2ObjectMap<IntArrayList>, IntArrayList>> skeleton = createSkeleton(partitions);

        // TODO
        // We assume that k/b of all paths go through one border node
        // Therefore we compute Highwayness for all border nodes and factorize the length with 1/k

        final SparseFunction<Integer, Double> ebtw = new HighwaynessLength(skeleton.x.graph, skeleton.x.weights).computeParallel();

        final Int2ObjectMap<IntArrayList> contractions = skeleton.y.x;
        final Int2DoubleMap fullHpr = new Int2DoubleOpenHashMap();

        for (final Pair<Integer, Double> pair: ebtw.sparsePair()) {
            final double weight = pair.y;

            for (final int e: contractions.get(pair.x.intValue())) {
                fullHpr.put(e, weight);
            }
        }

        return new MapSparseFunction<>(fullHpr);
    }

    private IntSet getSkeletonEdges(final Pair<IntSet, IntSet>[] partitions) {
        final IntSet skeletonEdgesOriginal = new IntOpenHashSet();

        // Go through each partition individually and take its local skeleton
        for (final Pair<IntSet, IntSet> partition: partitions) {
            final Map<Integer, Double> skeletonWeights = new HashMap<>();
            final GraphWithWeights<Double> skeleton = new GraphWithWeights<>(
                    new FixedGrph(),
                    new MapSparseFunction<>(skeletonWeights)
            );

            final Int2IntMap nodeMapping = new Int2IntOpenHashMap(partition.x.size());
            final Int2IntMap edgeMapping = new Int2IntOpenHashMap(6 * partition.x.size());
            final IntArrayList edgeBackMapping = new IntArrayList(6 * partition.x.size());

            // Create nodes and store mapping
            for (final int vM: partition.x) {
                final int v = skeleton.graph.addVertex();
                nodeMapping.put(vM, v);
            }

            final IntSet borderNodes = new IntOpenHashSet(partition.y.size());
            for (final int bM: partition.y) {
                borderNodes.add(nodeMapping.get(bM));
            }

            for (final int vM: partition.x) {
                final int v = nodeMapping.get(vM);

                for (final int eM: g.graph.getOutEdges(vM)) {
                    if (!edgeMapping.containsKey(eM)) {
                        final int wM = g.graph.getVerticesAccessibleThrough(vM, eM).iterator().nextInt();

                        if (nodeMapping.containsKey(wM)) {
                            final int w = nodeMapping.get(wM);
                            final boolean directed = g.graph.isDirectedSimpleEdge(eM);

                            final int e = skeleton.graph.addSimpleEdge(v, w, directed);

                            assert e == edgeBackMapping.size();

                            edgeMapping.put(eM, e);
                            edgeBackMapping.add(eM);
                            skeletonWeights.put(e, g.weights.apply(eM));
                        }
                    }
                }
            }

            // Compute skeleton from partition skeletons
            // Shortest paths between border nodes might leave partition
            // But then the combined skeleton of all partitions would still yield the SP between
            // the border nodes of one partition
            final IntSet spEdges = new MultiPathEdges().compute(skeleton, borderNodes, borderNodes);

            for (final int e: spEdges) {
                skeletonEdgesOriginal.add(edgeBackMapping.getInt(e));
            }

            // Add connections to other partitions
            for (final int bM: partition.y) {
                for (final int xM: g.graph.getNeighbours(bM)) {
                    if (!partition.x.contains(xM)) {
                        skeletonEdgesOriginal.addAll(
                                LucIntSets.intersection(g.graph.getEdgesIncidentTo(bM), g.graph.getEdgesIncidentTo(xM))
                        );
                    }
                }
            }
        }

        return skeletonEdgesOriginal;
    }

    private Pair<GraphWithWeights<Double>, Pair<Int2ObjectMap<IntArrayList>, IntArrayList>> createSkeleton(final Pair<IntSet, IntSet>[] partitions) {
        final int n = g.graph.getNumberOfVertices();

        // Get skeleton edges of all partitions
        final IntSet skeletonEdgesOriginal = getSkeletonEdges(partitions);

        System.out.println("Number of skeleton edges: " + skeletonEdgesOriginal.size());

        // Build a graph from all the partition skeletons
        final Int2DoubleMap weights = new Int2DoubleOpenHashMap(skeletonEdgesOriginal.size());
        final GraphWithWeights<Double> skeleton = new GraphWithWeights<>(new FixedGrph(), new MapSparseFunction<>(weights));

        final Int2IntMap nodeBackMapping = new Int2IntOpenHashMap(n);

        for (final int eM: skeletonEdgesOriginal) {
            final boolean directed = g.graph.isDirectedSimpleEdge(eM);

            final int vM;
            final int wM;

            if (directed) {
                vM = g.graph.getDirectedSimpleEdgeTail(eM);
                wM = g.graph.getDirectedSimpleEdgeHead(eM);
            } else {
                final IntSet vertices = g.graph.getVerticesIncidentToEdge(eM);

                if (vertices.size() != 2) {
                    throw new IllegalStateException("Only simple edges are allowed");
                }

                final IntIterator it = vertices.iterator();

                vM = it.nextInt();
                wM = it.nextInt();
            }

            skeleton.graph.addSimpleEdge(vM, eM, wM, directed);
            weights.put(eM, g.weights.apply(eM).doubleValue());
        }

        final IntSet borderNodes = new IntOpenHashSet();

        for (final Pair<IntSet, IntSet> partition: partitions) {
            borderNodes.addAll(partition.y);
        }

        // Contract vertices
        Int2ObjectMap<IntArrayList> contractions = new Int2ObjectOpenHashMap<>();

        for (final int v: skeleton.graph.getVertices()) {
            if (!borderNodes.contains(v)) {
                final IntSet edges = skeleton.graph.getEdgesIncidentTo(v);

                if (edges.size() == 2) {
                    final int[] es = edges.toIntArray();

                    final int u;
                    final int w;
                    final int e;

                    if (skeleton.graph.isUndirectedSimpleEdge(es[0]) && skeleton.graph.isUndirectedSimpleEdge(es[1])) {
                        final IntSet neighbors = skeleton.graph.getNeighbours(v);
                        final IntIterator it = neighbors.iterator();

                        u = it.nextInt();
                        w = it.nextInt();
                        e = skeleton.graph.addSimpleEdge(u, w, false);
                    } else if (
                            skeleton.graph.isDirectedSimpleEdge(es[0]) && skeleton.graph.isDirectedSimpleEdge(es[1])
                            && skeleton.graph.getInEdgeDegree(v) == 1 && skeleton.graph.getOutEdgeDegree(v) == 1
                    ) {
                        u = skeleton.graph.getInNeighbors(v).iterator().nextInt();
                        w = skeleton.graph.getOutNeighbors(v).iterator().nextInt();
                        e = skeleton.graph.addSimpleEdge(u, w,true);
                    } else {
                        continue;
                    }

                    // TODO: contraction can lead to multiple edges between two vertices
                    // TODO: they have to be merged
                    // If existing one is directed: replace all with directed and go for lowest directional edge weight

                    final double weight = skeleton.weights.apply(es[0]) + skeleton.weights.apply(es[1]);

                    weights.put(e, weight);
                    skeleton.graph.removeVertex(v);

                    IntArrayList contraction = new IntArrayList();

                    if (contractions.containsKey(es[0])) {
                        contraction.addAll(contractions.get(es[0]));
                        contractions.remove(es[0]);
                    } else {
                        contraction.add(es[0]);
                    }

                    if (contractions.containsKey(es[1])) {
                        contraction.addAll(contractions.get(es[1]));
                        contractions.remove(es[1]);
                    } else {
                        contraction.add(es[1]);
                    }

                    contractions.put(e, contraction);
                }
            }
        }

        System.out.println("Building final skeleton graph");

        final IntArrayList finalBorderNodes = new IntArrayList(borderNodes.size());
        final Int2DoubleMap finalWeights = new Int2DoubleOpenHashMap();
        final GraphWithWeights<Double> finalSkeleton = new GraphWithWeights<>(new FixedGrph(), new MapSparseFunction<>(finalWeights));
        final Int2IntMap finalNodeMapping = new Int2IntOpenHashMap();
        Int2ObjectMap<IntArrayList> finalContractions = new Int2ObjectOpenHashMap<>();

        for (final int vM: skeleton.graph.getVertices()) {
            final int v = finalSkeleton.graph.addVertex();
            finalNodeMapping.put(vM, v);

            if (borderNodes.contains(vM)) {
                finalBorderNodes.add(v);
            }
        }

        for (final int vM: skeleton.graph.getVertices()) {
            final int v = finalNodeMapping.get(vM);

            for (final int wM: skeleton.graph.getOutNeighbors(vM)) {
                final int w = finalNodeMapping.get(wM);

                // Only take one edge from v to w, they all have the same length otherwise they would
                // Not be  shortest paths between its end points
                // (which is a requirement to be in the skeleton, expect for the connecting ones between border nodes
                // where there should also be only one because the original graph is simple, multiple edges only appear
                // because of the contraction)
                if (finalSkeleton.graph.getEdgesConnecting(v, w).size() == 0) {
                    final IntSet edges = skeleton.graph.getEdgesConnecting(vM, wM);
                    int eM = -1;

                    for (final int edge : edges) {
                        eM = edge;

                        if (skeleton.graph.isUndirectedSimpleEdge(edge)) {
                            break;
                        }
                    }

                    final boolean directed = skeleton.graph.isDirectedSimpleEdge(eM);

                    final int e = finalSkeleton.graph.addSimpleEdge(v, w, directed);
                    finalWeights.put(e, weights.get(eM));

                    if (contractions.containsKey(eM)) {
                        finalContractions.put(e, contractions.get(eM));
                    } else {
                        finalContractions.put(e, new IntArrayList(new int[]{eM}));
                    }
                }
            }
        }

        System.out.println("Number of final skeleton nodes: " + skeleton.graph.getNumberOfVertices());
        System.out.println("Number of final skeleton edges: " + skeleton.graph.getNumberOfEdges());

        return new Pair<>(finalSkeleton, new Pair<>(finalContractions, finalBorderNodes));
    }

    private Pair<IntSet, IntSet>[] createPartitions(final int k) {
        final int n = g.graph.getNumberOfVertices();

        // Create partitions
        final IntSet remainingNodes = new IntOpenHashSet();
        final int[] nodeOrder = new int[n];

        for (int v = 0; v < n; v++) {
            remainingNodes.add(v);
            nodeOrder[v] = v;
        }

        shuffle(nodeOrder);

        final Int2IntMap nodePartitions = new Int2IntOpenHashMap();
        final ArrayList<Pair<IntSet, IntSet>> partitions = new ArrayList<>(k);
        final ArrayList<Pair<IntSet, IntSet>> smallPartitions = new ArrayList<>();

        // Create partitions from remaining non-partitioned nodes
        for (int i = 0; i < n && !remainingNodes.isEmpty(); i++) {
            final int s = nodeOrder[i];

            if (!remainingNodes.contains(s)) {
                continue;
            }

            final Pair<IntSet, IntSet> nh = findPartition(s, k, remainingNodes);
            final int partitionId = partitions.size();

            remainingNodes.removeAll(nh.x);
            partitions.add(nh);

            // Keep track of small partitions: they will be merged later on with their neighboring partitions
            if (nh.x.size() < k / 4) {
                smallPartitions.add(nh);
            }

            for (final int v: nh.x) {
                nodePartitions.put(v, partitionId);
            }
        }

        System.out.println("Total partitions: " + partitions.size());
        System.out.println("Small partitions: " + smallPartitions.size());

        // Merge small partitions into other ones
        for (final Pair<IntSet, IntSet> partition: smallPartitions) {
            // Find smallest neighbor partition
            int minNeighborSize = Integer.MAX_VALUE;
            int minPartitionId = -1;

            int partitionId = -1;

            for (final int b: partition.y) {
                if (partitionId == -1) {
                    partitionId = nodePartitions.get(b);
                }

                for (final int v: g.graph.getNeighbours(b)) {
                    final int neighborPartitionId = nodePartitions.get(v);

                    if (partitionId != neighborPartitionId) {
                        final int neighborSize = partitions.get(neighborPartitionId).x.size();

                        if (neighborSize < minNeighborSize) {
                            minNeighborSize = neighborSize;
                            minPartitionId = neighborPartitionId;
                        }
                    }
                }
            }

            if (minPartitionId == -1) {
                throw new IllegalStateException();
            }

            // Merge partition into other one
            Pair<IntSet, IntSet> minNeighbor = partitions.get(minPartitionId);

            final IntSet resultNeighborhood = minNeighbor.x;
            resultNeighborhood.addAll(partition.x);

            final IntSet resultBorder = minNeighbor.y;
            resultBorder.addAll(partition.y);

            final IntList mergedBorder = new IntArrayList();

            {
                final int partitionIdF = partitionId;
                final int minPartitionIdF = minPartitionId;

                for (final int b : resultBorder) {
                    // New border nodes are old border nodes excluding
                    // Those that were border nodes between each other
                    if (g.graph.getNeighbours(b).stream()
                            .mapToInt(v -> nodePartitions.get((int) v))
                            .filter(p -> p != partitionIdF && p != minPartitionIdF)
                            .count() == 0) {
                        mergedBorder.add(b);
                    }
                }
            }

            resultBorder.removeAll(mergedBorder);

            assert resultNeighborhood.containsAll(resultBorder);

            for (final int v: partition.x) {
                nodePartitions.put(v, minPartitionId);
            }
            partitions.set(partitionId, null);
        }

        // All inner nodes are only connected with nodes of the own partition
        assert g.graph.getVertices().stream().allMatch(vM -> {
            final int vp = nodePartitions.get(vM.intValue());

            if (partitions.get(vp).y.contains(vM.intValue())) {
                return true;
            } else {
                return g.graph.getNeighbours(vM).stream().allMatch(wM -> nodePartitions.get(wM.intValue()) == vp);
            }
        });

        // Nodes are not part of multiple partitions
        assert partitions.stream().filter(Objects::nonNull).map(p -> p.x).reduce((a, b) -> {
            final IntSet res = new IntOpenHashSet(a);
            res.retainAll(b);
            return res;
        }).get().size() == 0;

        // Every node is one partition (given that they appear not more than once)
        assert partitions.stream().filter(Objects::nonNull).mapToInt(p -> p.x.size()).sum() == n;

        System.out.println("Final partitions: " + partitions.stream().filter(Objects::nonNull).count());
        System.out.println("Total border nodes: " + partitions.stream().filter(Objects::nonNull).mapToInt(p -> p.y.size()).sum());

        return partitions.stream().filter(Objects::nonNull).toArray(Pair[]::new);
    }

    private void shuffle(final int[] array) {
        final Random r = new Random();
        r.setSeed(1235423743);

        for (int i = 0; i < array.length; i++) {
            final int j = r.nextInt(array.length);
            final int tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
    }

    private Pair<IntSet, IntSet> findPartition(final int s, final int k, final IntSet remainingNodes) {
        final int n = g.graph.getNumberOfVertices();
        final IntSet partition = new IntOpenHashSet();
        final IntSet border = new IntOpenHashSet();

        double minBorderSizeRation = Double.POSITIVE_INFINITY;
        IntSet minBorderPartition = partition;
        IntSet minBorder = border;

        // Initialization
        final IntSet settled = new IntOpenHashSet();
        final Int2DoubleMap totalWeight = new Int2DoubleOpenHashMap();
        totalWeight.defaultReturnValue(Double.POSITIVE_INFINITY);
        final IntSet stalled = new IntOpenHashSet(); // Nodes that lie behind a node of another partition

        Queue<Pair<Integer, Double>> Q = new PriorityQueue<>(Comparator.comparingDouble(o -> o.y));

        totalWeight.put(s, 0.0);
        Q.add(new Pair<>(s, 0.0));
        border.add(s);

        int l = 0;

        // TODO: abort exploration early if all remaining out-going connections are stalled!

        while (!Q.isEmpty() && l < k) {
            l++;

            final int v = Q.remove().x;

            if (settled.contains(v)) {
                continue;
            }

            settled.add(v);

            if (!remainingNodes.contains(v)) {
                stalled.add(v);
            }

            if (!stalled.contains(v)) {
                partition.add(v);


                // Partition has new vertex v: might be border node
                // and might make another vertex not border node any more
                for (final int w: g.graph.getNeighbours(v)) {
                    if (partition.contains(w)) {
                        if (partition.containsAll(g.graph.getNeighbours(w))) {
                            border.remove(w);
                        }
                    } else {
                        border.add(v);
                    }
                }
            }

            final double wv = totalWeight.get(v);

            for (int e: g.graph.getEdgesIncidentTo(v)) {
                final IntSet endpoints = g.graph.getVerticesIncidentToEdge(e);
                endpoints.remove(v);

                if (endpoints.size() != 1) {
                    throw new IllegalStateException("Only simple edges are allowed");
                }

                final int w = endpoints.iterator().nextInt();
                final double potentialWeight = wv + g.weights.apply(e);

                // Path discovery
                if (totalWeight.get(w) > potentialWeight) {
                    if (stalled.contains(v)) {
                        stalled.add(w);
                    }

                    totalWeight.put(w, potentialWeight);
                    Q.add(new Pair<>(w, potentialWeight));
                }

                if (totalWeight.get(w) == potentialWeight && w != s) {
                    // On alternative path: can be reached without another partition? Then target is not stalled
                    if (!stalled.contains(v)) {
                        stalled.remove(w);
                    }
                }
            }

            // Get partition between k / 2 and k with the least border node ratio
            // TODO: border size or partition size / border size ratio?
            if (l > k / 2 && partition.size() > 0) {
                final double borderSizeRatio = (double)border.size() / (double)partition.size();

                if (borderSizeRatio < minBorderSizeRation) {
                    minBorderSizeRation = borderSizeRatio;
                    minBorderPartition = new IntOpenHashSet(partition);
                    minBorder = new IntOpenHashSet(border);
                }
            }
        }

        final Pair<IntSet, IntSet> res = new Pair<>(minBorderPartition, minBorder);

        assert minBorderPartition.stream().allMatch(vM -> !stalled.contains(vM.intValue()))
                : "Partition nodes are not stalled";

        assert minBorderPartition.containsAll(minBorder)
                : "Border nodes are part of partition";

        assert minBorderPartition.stream().allMatch(
                vM -> res.y.contains(vM.intValue()) ||
                        res.x.containsAll(g.graph.getNeighbours(vM)))
                : "Inner nodes are only connected to nodes of same partition";

        return res;
    }
}
