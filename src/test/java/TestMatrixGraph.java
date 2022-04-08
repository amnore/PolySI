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
        var graph = new MatrixGraph<>(generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density)));
        System.err.printf("density: %g\n", density);

        var sparseTimer = Stopwatch.createStarted();
        var sparse = graph.composition("sparse", graph);
        System.err.printf("sparse: %s\n", sparseTimer.elapsed());

        var denseTimer = Stopwatch.createStarted();
        var dense = graph.composition("dense", graph);
        System.err.printf("dense: %s\n", denseTimer.elapsed());

        assertEquals(sparse, dense);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 1e-3, 5e-3, 1e-2, 5e-2 })
    void testReachability(double density) {
        var graph = new MatrixGraph<>(generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density)));
        System.err.printf("density: %g\n", density);

        var sparseTimer = Stopwatch.createStarted();
        var sparse = graph.reachability("sparse");
        System.err.printf("sparse: %s\n", sparseTimer.elapsed());

        var denseTimer = Stopwatch.createStarted();
        var dense = graph.reachability("dense");
        System.err.printf("dense: %s\n", denseTimer.elapsed());

        assertEquals(sparse, dense);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 1e-2, 5e-2, 0.1, 0.2, 0.5 })
    void testPreprocessingComposition(double density) {
        var graph = new PreprocessingMatrixGraph<>(
                generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density)), false);
        System.err.printf("density: %g\n", density);

        var sparseTimer = Stopwatch.createStarted();
        var sparse = graph.composition("sparse", graph);
        System.err.printf("sparse: %s\n", sparseTimer.elapsed());

        var denseTimer = Stopwatch.createStarted();
        var dense = graph.composition("dense", graph);
        System.err.printf("dense: %s\n", denseTimer.elapsed());

        assertEquals(sparse, dense);
    }

    @ParameterizedTest
    @ValueSource(doubles = { 1e-3, 5e-3, 1e-2, 5e-2 })
    void testPreprocessingReachability(double density) {
        var graph = new PreprocessingMatrixGraph<>(
                generateGraph(MATRIX_NODES, (int) (MATRIX_NODES * MATRIX_NODES * density)), false);
        System.err.printf("density: %g\n", density);

        var sparseTimer = Stopwatch.createStarted();
        var sparse = graph.reachability("sparse");
        System.err.printf("sparse: %s\n", sparseTimer.elapsed());

        var denseTimer = Stopwatch.createStarted();
        var dense = graph.reachability("dense");
        System.err.printf("dense: %s\n", denseTimer.elapsed());

        assertEquals(sparse, dense);
    }
}
