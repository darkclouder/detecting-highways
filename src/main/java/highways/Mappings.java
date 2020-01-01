package highways;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class Mappings {
    // Mapping node id to idx in graph
    public final Long2IntMap nodeMapping;
    // Mapping edge id to idx in graph
    public final Int2IntMap edgeMapping;

    public Mappings(final Long2IntMap nodeMapping, final Int2IntMap edgeMapping) {
        this.nodeMapping = nodeMapping;
        this.edgeMapping = edgeMapping;
    }

    public long[] nodeReverseMapping() {
        final long[] reverseMapping = new long[nodeMapping.size()];

        for (final long key: nodeMapping.keySet()) {
            reverseMapping[nodeMapping.get(key)] = key;
        }

        return reverseMapping;
    }

    public int[] edgeReverseMapping() {
        final int[] reverseMapping = new int[edgeMapping.size()];

        for (final int key: edgeMapping.keySet()) {
            reverseMapping[edgeMapping.get(key)] = key;
        }

        return reverseMapping;
    }

    public static Mappings oneMapping(final int n, final int m) {
        final Long2IntMap nodeMap = new Long2IntOpenHashMap(n);
        final Int2IntMap edgeMap = new Int2IntOpenHashMap(m);

        for (int i = 0; i < n; i++) {
            nodeMap.put(i, i);
        }

        for (int i = 0; i < m; i++) {
            edgeMap.put(i, i);
        }

        return new Mappings(nodeMap, edgeMap);
    }

    public static Mappings idMapping(final IntSet vertexSet, final IntSet edgeSet) {
        final Long2IntMap nodeMap = new Long2IntOpenHashMap(vertexSet.size());
        final Int2IntMap edgeMap = new Int2IntOpenHashMap(edgeSet.size());

        for (final int v: vertexSet) {
            nodeMap.put(v, v);
        }

        for (final int e: edgeSet) {
            edgeMap.put(e, e);
        }

        return new Mappings(nodeMap, edgeMap);
    }
}
