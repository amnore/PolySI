package verifier;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import graph.KnownGraph;
import history.History;
import history.Transaction;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import util.Profiler;

@SuppressWarnings("UnstableApiUsage")
class SISolver<KeyType, ValueType> {
    private final Solver solver = new Solver();

    // The literals of the known graph
    private final Map<Lit, Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> knownLiterals = new HashMap<>();

    // The literals asserting that exactly one set of edges exists in the graph
    // for each constraint
    private final Map<Lit, SIConstraint<KeyType, ValueType>> constraintLiterals = new HashMap<>();

    private final LazyLitMatrix<Transaction<KeyType, ValueType>> graphALits = new LazyLitMatrix<>(
            solver);
    private final LazyLitMatrix<Transaction<KeyType, ValueType>> graphBLits = new LazyLitMatrix<>(
            solver);

    boolean solve() {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_SOLVER_SOLVE");
        var lits = Stream
                .concat(knownLiterals.keySet().stream(),
                        constraintLiterals.keySet().stream())
                .collect(Collectors.toList());
        var result = solver.solve(lits);
        profiler.endTick("SI_SOLVER_SOLVE");

        return result;
    }

    Pair<Collection<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>, Collection<SIConstraint<KeyType, ValueType>>> getConflicts() {
        var edges = new ArrayList<Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>>();
        var constraints = new ArrayList<SIConstraint<KeyType, ValueType>>();

        solver.getConflictClause().stream().map(Logic::not).forEach(lit -> {
            if (knownLiterals.containsKey(lit)) {
                edges.add(knownLiterals.get(lit));
            } else {
                constraints.add(constraintLiterals.get(lit));
            }
        });
        return Pair.of(edges, constraints);
    }

    /*
     * Construct SISolver from constraints
     *
     * First construct two graphs: 1. Graph A contains WR, WW and SO edges. 2.
     * Graph B contains RW edges.
     *
     * For each edge in A and B, create a literal for it. The edge exists in the
     * final graph iff. the literal is true.
     *
     * Then, construct a third graph C using A and B: If P -> Q in A and Q -> R
     * in B, then P -> R in C The literal of P -> R is ((P -> Q) and (Q -> R)).
     *
     * Lastly, we add graph A and C to monosat, resulting in the final graph.
     *
     * Literals that are passed as assumptions to monograph: 1. The literals of
     * WR, SO edges, because those edges always exist. 2. For each constraint, a
     * literal that asserts exactly one set of edges exist in the graph.
     */
    SISolver(History<KeyType, ValueType> history,
            KnownGraph<KeyType, ValueType> precedenceGraph,
            Collection<SIConstraint<KeyType, ValueType>> constraints) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_SOLVER_GEN_GRAPH_A_B");
        var graphA = createKnownGraph(history, precedenceGraph.getKnownGraphA(),
                graphALits);
        var graphB = createKnownGraph(history, precedenceGraph.getKnownGraphB(),
                graphBLits);
        profiler.endTick("SI_SOLVER_GEN_GRAPH_A_B");

        profiler.startTick("SI_SOLVER_GEN_GRAPH_A_UNION_C");
        var matA = new MatrixGraph<>(graphA.asGraph());
        var orderInSession = Utils.getOrderInSession(history);
        var minimalAUnionC = Utils.reduceEdges(
                matA.union(
                        matA.composition(new MatrixGraph<>(graphB.asGraph()))),
                orderInSession);
        var reachability = minimalAUnionC.reachability();

        var knownEdges = Utils.getKnownEdges(graphA, graphB, minimalAUnionC);

        addConstraints(constraints, graphA, graphB);
        List.of(Pair.of('A', graphA), Pair.of('B', graphB)).forEach(p -> {
            var g = p.getRight();
            var edgesSize = g.edges().stream()
                    .mapToInt(e -> g.edgeValue(e).get().size()).sum();
            System.err.printf("Graph %s edges count: %d\n", p.getLeft(),
                    edgesSize);
        });

        var unknownEdges = Utils.getUnknownEdges(graphA, graphB, reachability,
                solver);
        profiler.endTick("SI_SOLVER_GEN_GRAPH_A_UNION_C");

        System.err.printf("Graph A union C edges count: %d\n",
                knownEdges.size() + unknownEdges.size());

        profiler.startTick("SI_SOLVER_GEN_MONO_GRAPH");
        var monoGraph = new monosat.Graph(solver);
        var nodeMap = new HashMap<Transaction<KeyType, ValueType>, Integer>();

        history.getTransactions().forEach(n -> {
            nodeMap.put(n, monoGraph.addNode());
        });

        var addToMonoSAT = ((Consumer<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>) e -> {
            var n = e.getLeft();
            var s = e.getMiddle();
            solver.assertEqual(e.getRight(),
                    monoGraph.addEdge(nodeMap.get(n), nodeMap.get(s)));
        });

        knownEdges.forEach(addToMonoSAT);
        unknownEdges.forEach(addToMonoSAT);
        profiler.endTick("SI_SOLVER_GEN_MONO_GRAPH");

        solver.assertTrue(monoGraph.acyclic());
    }

    private MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> createKnownGraph(
            History<KeyType, ValueType> history,
            ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> knownGraph,
            LazyLitMatrix<Transaction<KeyType, ValueType>> litMatrix) {
        var g = Utils.createEmptyGraph(history);
        for (var e : knownGraph.edges()) {
            var lit = new Lit(solver);
            var edgeLit = litMatrix.get(e.source(), e.target());
            solver.assertEqual(lit, edgeLit);
            knownLiterals.put(lit, Pair.of(e, knownGraph.edgeValue(e).get()));
            Utils.addEdge(g, e.source(), e.target(), edgeLit);
        }

        return g;
    }

    private void addConstraints(
            Collection<SIConstraint<KeyType, ValueType>> constraints,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB) {
        var addEdges = ((Function<Collection<SIEdge<KeyType, ValueType>>, Pair<Lit, Lit>>) edges -> {
            // all means all edges exists in the graph.
            // Similar for none.
            var allLits = new ArrayList<Lit>();
            var noneLits = new ArrayList<Lit>();
            for (var e : edges) {
                Lit lit;
                if (e.getType().equals(EdgeType.WW)) {
                    lit = graphALits.get(e.getFrom(), e.getTo());
                    Utils.addEdge(graphA, e.getFrom(), e.getTo(), lit);
                } else {
                    lit = graphBLits.get(e.getFrom(), e.getTo());
                    Utils.addEdge(graphB, e.getFrom(), e.getTo(), lit);
                }

                allLits.add(lit);
                noneLits.add(Logic.not(lit));
            }

            var all = Logic.and(allLits.toArray(Lit[]::new));
            var none = Logic.and(noneLits.toArray(Lit[]::new));
            return Pair.of(all, none);
        });

        for (var c : constraints) {
            var p1 = addEdges.apply(c.getEdges1());
            var p2 = addEdges.apply(c.getEdges2());

            constraintLiterals
                    .put(Logic.or(Logic.and(p1.getLeft(), p2.getRight()),
                            Logic.and(p2.getLeft(), p1.getRight())), c);
        }
    }
}

@RequiredArgsConstructor
class LazyLitMatrix<T> {
    private final Map<Pair<T, T>, Lit> lits = new HashMap<>();
    private final Solver solver;

    public Lit get(T a, T b) {
        return lits.computeIfAbsent(Pair.of(a, b), p -> new Lit(solver));
    }
}
