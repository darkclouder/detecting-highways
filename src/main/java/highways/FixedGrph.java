package highways;

import grph.in_memory.InMemoryGrph;
import it.unimi.dsi.fastutil.ints.IntSet;
import toools.collections.LucIntSets;

public class FixedGrph extends InMemoryGrph {
    @Override
    public IntSet getEdgesConnecting(int src, int dest) {
        assert this.getVertices().contains(src);

        assert this.getVertices().contains(dest);

        return LucIntSets.intersection(this.getOutEdges(src), this.getInEdges(dest));
    }
}
