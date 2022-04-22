package verifier;

import graph.PrecedenceGraph;
import history.History;
import history.History.Session;
import history.History.Transaction;
import util.Profiler;
import graph.MatrixGraph;
import java.util.Collection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Streams;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

class Pruning {
    static <KeyType, ValueType> boolean pruneConstraints(String method,
            PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();
        var pruneFunc = ((BiFunction<PrecedenceGraph<KeyType, ValueType>, Collection<SIConstraint<KeyType, ValueType>>, Pair<Integer, Boolean>>) null);

        switch (method) {
        case "Post-BFS":
            pruneFunc = (g, c) -> pruneConstraintsWithPostChecking("sparse", g,
                    c, history);
            break;
        case "Post-Floyd":
            pruneFunc = (g, c) -> pruneConstraintsWithPostChecking("dense", g,
                    c, history);
            break;
        case "Pre-BFS":
            pruneFunc = (g, c) -> pruneConstraintsWithPreprocessing("sparse", g,
                    c, history);
            break;
        case "Pre-Floyd":
            pruneFunc = (g, c) -> pruneConstraintsWithPreprocessing("dense", g,
                    c, history);
            break;
        }

        profiler.startTick("SI_PRUNE");
        int rounds = 1, solvedConstraints = 0;
        while (true) {
            System.err.printf("Pruning round %d\n", rounds);
            var result = pruneFunc.apply(knownGraph, constraints);

            if (result.getRight()) {
                profiler.endTick("SI_PRUNE");
                return true;
            } else if (result.getLeft() == 0) {
                break;
            }

            solvedConstraints += result.getLeft();
            rounds++;
        }
        profiler.endTick("SI_PRUNE");

        System.err.printf(
                "Pruned %d rounds, solved %d constraints\n"
                        + "After prune: graphA: %d, graphB: %d\n",
                rounds, solvedConstraints,
                knownGraph.getKnownGraphA().edges().size(),
                knownGraph.getKnownGraphB().edges().size());
        return false;
    }

    private static <KeyType, ValueType> Pair<Integer, Boolean> pruneConstraintsWithPostChecking(
            String type, PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB());
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        var graphC = graphA.composition(type, graphB);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        if (graphC.hasLoops()) {
            return Pair.of(0, true);
        }

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        var reachability = PruningUtils
                .reduceEdges(graphA.union(graphC), history).reachability(type);
        profiler.endTick("SI_PRUNE_POST_REACHABILITY");

        var solvedConstraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        profiler.startTick("SI_PRUNE_POST_CHECK");
        for (var c : constraints) {
            var solveConflict = ((BiFunction<List<SIEdge<KeyType, ValueType>>, List<SIEdge<KeyType, ValueType>>, Boolean>) (
                    edges, other) -> {
                boolean hasConflict = false;
                outer: for (var e : edges) {
                    switch (e.getType()) {
                    case WW:
                        if (reachability.hasEdgeConnecting(e.getTo(),
                                e.getFrom())) {
                            hasConflict = true;
                            // System.err.printf("conflict edge: %s\n", e);
                            break outer;
                        }
                        break;
                    case RW:
                        for (var n : knownGraph.getKnownGraphA()
                                .predecessors(e.getFrom())) {
                            if (reachability.hasEdgeConnecting(e.getTo(), n)) {
                                hasConflict = true;
                                // System.err.printf("conflict edge: %s\n", e);
                                break outer;
                            }
                        }
                        break;
                    }
                }

                if (hasConflict) {
                    for (var e : other) {
                        switch (e.getType()) {
                        case WW:
                            knownGraph.putEdgeToGraphA(e.getFrom(), e.getTo());
                            break;
                        case RW:
                            knownGraph.putEdgeToGraphB(e.getFrom(), e.getTo());
                            break;
                        }
                    }
                }

                return hasConflict;
            });

            if (solveConflict.apply(c.edges1, c.edges2)
                    || solveConflict.apply(c.edges2, c.edges1)) {
                solvedConstraints.add(c);
            }
        }
        profiler.endTick("SI_PRUNE_POST_CHECK");

        // System.err.printf("solved constraints: %s\n", solvedConstraints);
        // constraints.removeAll(solvedConstraints);
        // java removeAll has performance bugs; do it manually
        solvedConstraints.forEach(constraints::remove);
        return Pair.of(solvedConstraints.size(), false);
    }

    private static <KeyType, ValueType> Pair<Integer, Boolean> pruneConstraintsWithPreprocessing(
            String type, PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB());
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        var graphC = graphA.composition(type, graphB);
        PruningUtils.reduceEdges(graphC, history);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        if (graphC.hasLoops()) {
            return Pair.of(0, true);
        }

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        var reachability = PruningUtils
                .reduceEdges(graphA.union(graphC), history).reachability(type);
        var RWReachability = reachability.composition(type, graphA);
        profiler.endTick("SI_PRUNE_POST_REACHABILITY");

        var solvedConstraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        profiler.startTick("SI_PRUNE_POST_CHECK");
        for (var c : constraints) {
            var solveConflict = ((BiFunction<List<SIEdge<KeyType, ValueType>>, List<SIEdge<KeyType, ValueType>>, Boolean>) (
                    edges, other) -> {
                boolean hasConflict = false;
                for (var e : edges) {
                    var hasInverseEdge = ((Function<SIEdge<KeyType, ValueType>, Boolean>) edge -> {
                        switch (edge.getType()) {
                        case WW:
                            return reachability.hasEdgeConnecting(edge.getTo(),
                                    edge.getFrom());
                        default:
                            return RWReachability.hasEdgeConnecting(
                                    edge.getTo(), edge.getFrom());
                        }
                    });

                    if (hasInverseEdge.apply(e)) {
                        // System.err.printf("conflict edge: %s\n", e);
                        hasConflict = true;
                        break;
                    }
                }

                if (hasConflict) {
                    for (var e : other) {
                        switch (e.getType()) {
                        case WW:
                            knownGraph.putEdgeToGraphA(e.getFrom(), e.getTo());
                            break;
                        case RW:
                            knownGraph.putEdgeToGraphB(e.getFrom(), e.getTo());
                            break;
                        }
                    }
                }

                return hasConflict;
            });

            if (solveConflict.apply(c.edges1, c.edges2)
                    || solveConflict.apply(c.edges2, c.edges1)) {
                solvedConstraints.add(c);
            }
        }
        profiler.endTick("SI_PRUNE_POST_CHECK");

        constraints.removeAll(solvedConstraints);
        return Pair.of(solvedConstraints.size(), false);
    }
}

class PruningUtils {
    static <KeyType, ValueType> MatrixGraph<Transaction<KeyType, ValueType>> reduceEdges(
            MatrixGraph<Transaction<KeyType, ValueType>> graph,
            History<KeyType, ValueType> history) {
        var orderInSession = Utils.getOrderInSession(history);

        System.err.printf("Before: %d edges\n", graph.edges().size());

        for (var n : graph.nodes()) {
            var firstInSession = new HashMap<Session<KeyType, ValueType>, Transaction<KeyType, ValueType>>();
            for (var m : graph.successors(n)) {
                var s = m.getSession();
                if (!firstInSession.containsKey(s)) {
                    firstInSession.put(s, m);
                } else {
                    var o = firstInSession.get(s);
                    if (orderInSession.get(o) < orderInSession.get(m)) {
                        graph.removeEdge(n, m);
                    } else {
                        firstInSession.put(s, m);
                        graph.removeEdge(n, o);
                    }
                }
            }
        }

        System.err.printf("After: %d edges\n", graph.edges().size());
        return graph;
    }
}
