package verifier;

import graph.PrecedenceGraph;
import history.History;
import history.Transaction;
import util.Profiler;
import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import java.util.Collection;
import java.util.ArrayList;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;

class Pruning {
    static <KeyType, ValueType> boolean pruneConstraints(PrecedenceGraph<KeyType, ValueType> knownGraph,
                                                         Collection<SIConstraint<KeyType, ValueType>> constraints,
                                                         History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE");
        int rounds = 1, solvedConstraints = 0;
        while (true) {
            System.err.printf("Pruning round %d\n", rounds);
            var result = pruneConstraintsWithPostChecking(knownGraph, constraints, history);

            if (result.getRight()) {
                profiler.endTick("SI_PRUNE");
                System.err.printf(
                        "Pruned %d rounds, solved %d constraints\n"
                                + "After prune: graphA: %d, graphB: %d\n",
                        rounds, solvedConstraints,
                        knownGraph.getKnownGraphA().edges().size(),
                        knownGraph.getKnownGraphB().edges().size());
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
            PrecedenceGraph<KeyType, ValueType> knownGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints,
            History<KeyType, ValueType> history) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_PRUNE_POST_GRAPH_A_B");
        var graphA = new MatrixGraph<>(knownGraph.getKnownGraphA().asGraph());
        var graphB = new MatrixGraph<>(knownGraph.getKnownGraphB().asGraph());
        var orderInSession = Utils.getOrderInSession(history);
        profiler.endTick("SI_PRUNE_POST_GRAPH_A_B");

        profiler.startTick("SI_PRUNE_POST_GRAPH_C");
        var graphC = graphA.composition(graphB);
        profiler.endTick("SI_PRUNE_POST_GRAPH_C");

        if (graphC.hasLoops()) {
            return Pair.of(0, true);
        }

        profiler.startTick("SI_PRUNE_POST_REACHABILITY");
        var reachability = Utils
                .reduceEdges(graphA.union(graphC), orderInSession)
                .reachability();
        profiler.endTick("SI_PRUNE_POST_REACHABILITY");

        var solvedConstraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        profiler.startTick("SI_PRUNE_POST_CHECK");
        for (var c : constraints) {
            var conflict = checkConflict(c.edges1, reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.edges2);
                solvedConstraints.add(c);
                // System.err.printf("%s -> %s because of conflict in %s\n",
                //         c.writeTransaction2, c.writeTransaction1,
                //         conflict.get());
                continue;
            }

            conflict = checkConflict(c.edges2, reachability, knownGraph);
            if (conflict.isPresent()) {
                addToKnownGraph(knownGraph, c.edges1);
                // System.err.printf("%s -> %s because of conflict in %s\n",
                //         c.writeTransaction1, c.writeTransaction2,
                //         conflict.get());
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

    private static <KeyType, ValueType> void addToKnownGraph(
            PrecedenceGraph<KeyType, ValueType> knownGraph,
            List<SIEdge<KeyType, ValueType>> edges) {
        for (var e : edges) {
            switch (e.getType()) {
            case WW:
                knownGraph.putEdge(e.getFrom(), e.getTo(),
                        new Edge<KeyType>(EdgeType.WW, e.getKey()));
                break;
            case RW:
                knownGraph.putEdge(e.getFrom(), e.getTo(),
                        new Edge<KeyType>(EdgeType.RW, e.getKey()));
                break;
            default:
                throw new Error(
                        "only WW and RW edges should appear in constraints");
            }
        }
    }

    private static <KeyType, ValueType> Optional<SIEdge<KeyType, ValueType>> checkConflict(
            List<SIEdge<KeyType, ValueType>> edges,
            MatrixGraph<Transaction<KeyType, ValueType>> reachability,
            PrecedenceGraph<KeyType, ValueType> knownGraph) {
        for (var e : edges) {
            switch (e.getType()) {
            case WW:
                if (reachability.hasEdgeConnecting(e.getTo(), e.getFrom())) {
                    return Optional.of(e);
                    // System.err.printf("conflict edge: %s\n", e);
                }
                break;
            case RW:
                for (var n : knownGraph.getKnownGraphA()
                        .predecessors(e.getFrom())) {
                    if (reachability.hasEdgeConnecting(e.getTo(), n)) {
                        return Optional.of(e);
                        // System.err.printf("conflict edge: %s\n", e);
                    }
                }
                break;
            default:
                throw new Error(
                        "only WW and RW edges should appear in constraints");
            }
        }

        return Optional.empty();
    }

    private static <KeyType, ValueType> Optional<SIEdge<KeyType, ValueType>> checkConflict(
            List<SIEdge<KeyType, ValueType>> edges,
            MatrixGraph<Transaction<KeyType, ValueType>> reachability,
            MatrixGraph<Transaction<KeyType, ValueType>> RWReachability) {
        SIEdge<KeyType, ValueType> conflictEdge = null;
        var hasInverseEdge = ((Function<SIEdge<KeyType, ValueType>, Boolean>) edge -> {
            switch (edge.getType()) {
            case WW:
                return reachability.hasEdgeConnecting(edge.getTo(),
                        edge.getFrom());
            default:
                return RWReachability.hasEdgeConnecting(edge.getTo(),
                        edge.getFrom());
            }
        });

        for (var e : edges) {
            if (hasInverseEdge.apply(e)) {
                conflictEdge = e;
                // System.err.printf("conflict edge: %s\n", e);
                break;
            }
        }

        return conflictEdge == null ? Optional.empty()
                : Optional.of(conflictEdge);
    }
}

class PruningUtils {
}
