package verifier;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Streams;

import com.google.common.graph.EndpointPair;
import graph.Edge;
import graph.EdgeType;
import graph.KnownGraph;
import history.History;
import history.Transaction;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import org.apache.commons.lang3.tuple.Pair;

class SISolver2<KeyType, ValueType> {
    private Lit[][] graphABEdges;
    private Lit[][] graphACEdges;
    private BiMap<Transaction<KeyType, ValueType>, Integer> nodeMap;

    private Solver solver = new Solver();

    private HashSet<Lit> knownLits = new HashSet<>();
    private HashSet<Lit> constraintLits = new HashSet<>();

    SISolver2(History<KeyType, ValueType> history,
            KnownGraph<KeyType, ValueType> precedenceGraph,
            Set<SIConstraint<KeyType, ValueType>> constraints) {
        var nodeMap = new HashMap<Transaction<KeyType, ValueType>, Integer>();
        {
            int i = 0;
            for (var txn : history.getTransactions()) {
                nodeMap.put(txn, i++);
            }
        }

        var createLitMatrix = ((Supplier<Lit[][]>) () -> {
            var n = history.getTransactions().size();
            var lits = new Lit[n][n];
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    lits[j][k] = new Lit(solver);
                }
            }

            return lits;
        });
        graphABEdges = createLitMatrix.get();
        graphACEdges = createLitMatrix.get();

        for (var e : precedenceGraph.getKnownGraphA().edges()) {
            knownLits.add(graphABEdges[nodeMap.get(e.source())][nodeMap
                    .get(e.target())]);
        }
        for (var e : precedenceGraph.getKnownGraphB().edges()) {
            knownLits.add(graphABEdges[nodeMap.get(e.source())][nodeMap
                    .get(e.target())]);
        }

        for (var c : constraints) {
            var either = Logic.implies(
                    graphABEdges[nodeMap.get(c.getWriteTransaction1())][nodeMap
                            .get(c.getWriteTransaction2())],
                    c.getEdges1().stream()
                            .filter(e -> e.getType().equals(EdgeType.RW))
                            .map(e -> graphABEdges[nodeMap
                                    .get(e.getFrom())][nodeMap.get(e.getTo())])
                            .reduce(Lit.True, Logic::and));
            var or = Logic.implies(
                    graphABEdges[nodeMap.get(c.getWriteTransaction2())][nodeMap
                            .get(c.getWriteTransaction1())],
                    c.getEdges2().stream()
                            .filter(e -> e.getType().equals(EdgeType.RW))
                            .map(e -> graphABEdges[nodeMap
                                    .get(e.getFrom())][nodeMap.get(e.getTo())])
                            .reduce(Lit.True, Logic::and));

            constraintLits.add(Logic.or(either, or));
        }

        var edgesInA = constraints.stream()
                .flatMap(c -> Stream.concat(c.getEdges1().stream(),
                        c.getEdges2().stream()))
                .filter(e -> e.getType().equals(EdgeType.WW))
                .collect(Collectors.toList());
        var edgesInB = constraints.stream()
                .flatMap(c -> Stream.concat(c.getEdges1().stream(),
                        c.getEdges2().stream()))
                .filter(e -> e.getType().equals(EdgeType.RW))
                .collect(Collectors.toList());

        for (var e1 : edgesInA) {
            for (var e2 : edgesInB) {
                if (!e1.getTo().equals(e2.getFrom())) {
                    continue;
                }

                var vi = nodeMap.get(e1.getFrom());
                var vj = nodeMap.get(e1.getTo());
                var vk = nodeMap.get(e2.getTo());
                solver.assertEqual(graphACEdges[vi][vk],
                        Logic.and(graphABEdges[vi][vj], graphABEdges[vj][vk]));
            }
        }

        var monoGraph = new monosat.Graph(solver);
        var nodes = new int[graphACEdges.length];

        for (int i = 0; i < graphACEdges.length; i++) {
            nodes[i] = monoGraph.addNode();
        }
        for (int i = 0; i < graphACEdges.length; i++) {
            for (int j = 0; j < graphACEdges[i].length; j++) {
                var lit = monoGraph.addEdge(nodes[i], nodes[j]);
                solver.assertEqual(lit, graphACEdges[i][j]);
            }
        }

        solver.assertTrue(monoGraph.acyclic());
    }

    public boolean solve() {
        var lits = Stream.concat(knownLits.stream(), constraintLits.stream())
            .collect(Collectors.toList());

        return solver.solve(lits);
    }

    Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>, Collection<SIConstraint<KeyType, ValueType>>> getConflicts() {
        return Pair.of(Collections.emptyList(), Collections.emptyList());
    }
}
