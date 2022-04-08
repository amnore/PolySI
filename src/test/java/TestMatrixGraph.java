import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.util.List;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;

import graph.MatrixGraph;
import graph.PreprocessingMatrixGraph;

class TestMatrixGraph {
    private static final int MATRIX_NODES = 500;

    private Graph<Integer> generateGraph(int nodeNum, int edgesNum) {
        MutableGraph<Integer> graph = GraphBuilder.directed().allowsSelfLoops(true).build();

        IntStream.range(0, nodeNum).forEach(n -> graph.addNode(n));

        Streams.zip(RandomGenerator.getDefault().ints(0, nodeNum).boxed(),
                RandomGenerator.getDefault().ints(0, nodeNum).boxed(), Pair::of)
                .limit(edgesNum)
                .forEach(p -> graph.putEdge(p.getLeft(), p.getRight()));

        return graph;
    }

    @ParameterizedTest
    @ValueSource(doubles = { 1e-2, 5e-2, 0.1, 0.2, 0.5 })
    void testComposition(double density) {
        var graph = generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density));
        var g = new MatrixGraph<>(graph);
        var pg = new PreprocessingMatrixGraph<>(graph, false);
        System.err.printf("density: %g\n", density);

        var t = Stopwatch.createStarted();
        var sparse = g.composition("sparse", g);
        System.err.printf("sparse: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var dense = g.composition("dense", g);
        System.err.printf("dense: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var psparse = pg.composition("sparse", pg);
        System.err.printf("prepricessing sparse: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var pdense = pg.composition("dense", pg);
        System.err.printf("preprocessing dense: %s\n", t.elapsed());

        assertEquals(sparse, dense);
        assertEquals(psparse, pdense);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 1e-3, 5e-3, 1e-2, 5e-2 })
    void testReachability(double density) {
        var graph = generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density));
        var g = new MatrixGraph<>(graph);
        var pg = new PreprocessingMatrixGraph<>(graph, false);
        System.err.printf("density: %g\n", density);

        var t = Stopwatch.createStarted();
        var sparse = g.reachability("sparse");
        System.err.printf("sparse: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var dense = g.reachability("dense");
        System.err.printf("dense: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var psparse = pg.reachability("sparse");
        System.err.printf("preprocessing sparse: %s\n", t.elapsed());

        t = Stopwatch.createStarted();
        var pdense = pg.reachability("dense");
        System.err.printf("preprocessing dense: %s\n", t.elapsed());

        assertEquals(sparse, dense);
        assertEquals(psparse, pdense);
    }
}
