package verifier;

import graph.PrecedenceGraph;
import util.Profiler;
import graph.PreprocessingMatrixGraph;
import graph.MatrixGraph;
import java.util.Collection;
import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.List;

class Pruning {
    static <KeyType, ValueType> void pruneConstraints(String method, PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var profiler = Profiler.getInstance();
        var pruneFunc = ((BiFunction<PrecedenceGraph<KeyType, ValueType>, Collection<SIConstraint<KeyType, ValueType>>, Integer>) null);

        switch (method) {
            case "Post-BFS":
                pruneFunc = (g, c) -> pruneConstraintsWithPostChecking("sparse", g, c);
                break;
            case "Post-Floyd":
                pruneFunc = (g, c) -> pruneConstraintsWithPostChecking("dense", g, c);
                break;
            case "Pre-BFS":
                pruneFunc = (g, c) -> pruneConstraintsWithPreprocessing("sparse", g, c);
                break;
            case "Pre-Floyd":
                pruneFunc = (g, c) -> pruneConstraintsWithPreprocessing("dense", g, c);
                break;
        }

        profiler.startTick("SI_PRUNE");
        int rounds = 1, solvedConstraints = 0, solvedThisRound;
        System.err.printf("Pruning");
        while ((solvedThisRound = pruneFunc.apply(knownGraph, constraints)) != 0) {
            System.err.printf(".");
            solvedConstraints += solvedThisRound;
            rounds++;
            continue;
        }
        profiler.endTick("SI_PRUNE");

        System.err.printf("Pruned %d rounds, solved %d constraints\n"
                + "After prune: graphA: %d, graphB: %d\n",
                rounds, solvedConstraints,
                knownGraph.getKnownGraphA().edges().size(),
                knownGraph.getKnownGraphB().edges().size());
    }

    private static <KeyType, ValueType> int pruneConstraintsWithPostChecking(
            String type,
            PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB());
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        var graphC = graphA.composition(type, graphB);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        var reachability = graphA.union(graphC).reachability(type);
        profiler.endTick("SI_PRUNE_POST_REACHABILITY");

        var solvedConstraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        profiler.startTick("SI_PRUNE_POST_CHECK");
        for (var c : constraints) {
            var solveConflict = ((BiFunction<List<SIEdge<KeyType, ValueType>>, List<SIEdge<KeyType, ValueType>>, Boolean>) (
                    edges,
                    other) -> {
                boolean hasConflict = false;
                outer: for (var e : edges) {
                    switch (e.getType()) {
                        case WW:
                            if (reachability.hasEdgeConnecting(e.getTo(), e.getFrom())) {
                                hasConflict = true;
                                // System.err.printf("conflict edge: %s\n", e);
                                break outer;
                            }
                            break;
                        case RW:
                            for (var n : knownGraph.getKnownGraphA().predecessors(e.getFrom())) {
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

            if (solveConflict.apply(c.edges1, c.edges2) ||
                    solveConflict.apply(c.edges2, c.edges1)) {
                solvedConstraints.add(c);
            }
        }
        profiler.endTick("SI_PRUNE_POST_CHECK");

        // System.err.printf("solved constraints: %s\n", solvedConstraints);
        constraints.removeAll(solvedConstraints);
        return solvedConstraints.size();
    }

    private static <KeyType, ValueType> int pruneConstraintsWithPreprocessing(
            String type,
            PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB());
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        var graphC = graphA.composition(type, graphB);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        var reachability = graphA.union(graphC).reachability(type);
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
                                return reachability.hasEdgeConnecting(edge.getTo(), edge.getFrom());
                            default:
                                return RWReachability.hasEdgeConnecting(edge.getTo(), edge.getFrom());
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

            if (solveConflict.apply(c.edges1, c.edges2) || solveConflict.apply(c.edges2, c.edges1)) {
                solvedConstraints.add(c);
            }
        }
        profiler.endTick("SI_PRUNE_POST_CHECK");

        constraints.removeAll(solvedConstraints);
        return solvedConstraints.size();
    }
}
