package verifier;

import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.graph.*;

import graph.Edge;
import graph.EdgeType;
import graph.MatrixGraph;
import graph.PrecedenceGraph;
import history.Event;
import history.History;
import history.HistoryLoader;
import history.Transaction;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import util.Profiler;
import util.TriConsumer;

@SuppressWarnings("UnstableApiUsage")
public class SIVerifier<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    public SIVerifier(HistoryLoader<KeyType, ValueType> loader) {
        history = loader.loadHistory();
        System.err.printf(
                "Sessions count: %d\nTransactions count: %d\nEvents count: %d\n",
                history.getSessions().size(), history.getTransactions().size(),
                history.getEvents().size());
    }

    public boolean audit() {
        var profiler = Profiler.getInstance();

        profiler.startTick("ONESHOT_CONS");
        profiler.startTick("SI_VERIFY_INT");
        boolean satisfy_int = Utils.verifyInternalConsistency(history);
        profiler.endTick("SI_VERIFY_INT");
        if (!satisfy_int) {
            return false;
        }

        profiler.startTick("SI_GEN_PREC_GRAPH");
        var graph = new PrecedenceGraph<>(history);
        profiler.endTick("SI_GEN_PREC_GRAPH");
        System.err.printf("Known edges: %d\n",
                graph.getKnownGraphA().edges().size());

        profiler.startTick("SI_GEN_CONSTRAINTS");
        var constraints = generateConstraints(history, graph);
        profiler.endTick("SI_GEN_CONSTRAINTS");
        System.err.printf(
                "Constraints count: %d\nTotal edges in constraints: %d\n",
                constraints.size(),
                constraints.stream().map(c -> c.edges1.size() + c.edges2.size())
                        .reduce(Integer::sum).orElse(0));
        profiler.endTick("ONESHOT_CONS");

        var hasLoop = Pruning.pruneConstraints("Post-BFS", graph, constraints,
                history);
        if (hasLoop) {
            System.err.printf("Loop found in pruning\n");
        }

        var solver = new SISolver<>(history, graph, constraints);

        profiler.startTick("ONESHOT_SOLVE");
        boolean accepted = solver.solve();
        profiler.endTick("ONESHOT_SOLVE");

        if (!accepted) {
            var conflicts = solver.getConflicts();
            System.err.println("Conflicts:");
            conflicts.getLeft()
                    .forEach(p -> System.err.printf("Edge: %s\n", p));
            conflicts.getRight()
                    .forEach(c -> System.err.printf("Constraint: %s\n", c));

            var txns = new HashSet<Transaction<KeyType, ValueType>>();
            conflicts.getLeft().forEach(e -> {
                txns.add(e.getLeft().source());
                txns.add(e.getLeft().target());
            });
            conflicts.getRight().forEach(c -> {
                var addEdges = ((Consumer<List<SIEdge<KeyType, ValueType>>>) s -> s
                        .forEach(e -> {
                            txns.add(e.from);
                            txns.add(e.to);
                        }));
                addEdges.accept(c.edges1);
                addEdges.accept(c.edges2);
            });
            System.err.println("Relevent transactions:");
            txns.forEach(t -> {
                System.err.printf("sessionid: %d, id: %d\nops:\n",
                        t.getSession().getId(), t.getId());
                t.getEvents().forEach(e -> System.err.printf("%s %s = %s\n",
                        e.getType(), e.getKey(), e.getValue()));
            });
        }

        return accepted;
    }

    /*
     * Generate constraints from a precedence graph. Use coalescing to reduce
     * the number of constraints produced.
     *
     * @param graph the graph to use
     *
     * @return the set of constraints generated
     *
     * For each pair of transactions A, C, generate the following constraint:
     *
     * 1. A precedes C, add A ->(ww) C. Let K be a key written by both A and C,
     * for each transaction B such that A ->(wr, K) B, add B ->(rw) C.
     *
     * 2. C precedes A, add C ->(ww) A. For each transaction B such that C
     * ->(wr, K) A, add B ->(rw) A.
     */
    private static <KeyType, ValueType> Set<SIConstraint<KeyType, ValueType>> generateConstraints(
            History<KeyType, ValueType> history,
            PrecedenceGraph<KeyType, ValueType> graph) {
        var readFrom = graph.getReadFrom();
        var writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

        history.getEvents().stream()
                .filter(e -> e.getType() == Event.EventType.WRITE)
                .forEach(ev -> {
                    writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>())
                            .add(ev.getTransaction());
                });

        var forEachWriteSameKey = ((Consumer<TriConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, KeyType>>) f -> {
            for (var p : writes.entrySet()) {
                var key = p.getKey();
                var list = new ArrayList<>(p.getValue());
                for (int i = 0; i < list.size(); i++) {
                    for (int j = i + 1; j < list.size(); j++) {
                        f.accept(list.get(i), list.get(j), key);
                    }
                }
            }
        });

        var constraintEdges = new HashMap<Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>, List<SIEdge<KeyType, ValueType>>>();
        forEachWriteSameKey.accept((a, c, key) -> {
            var addEdge = ((BiConsumer<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>) (
                    m, n) -> {
                constraintEdges
                        .computeIfAbsent(Pair.of(m, n), p -> new ArrayList<>())
                        .add(new SIEdge<>(m, n, EdgeType.WW, key));
            });
            addEdge.accept(a, c);
            addEdge.accept(c, a);
        });

        for (var a : history.getTransactions()) {
            for (var b : readFrom.successors(a)) {
                for (var edge : readFrom.edgeValue(a, b).get()) {
                    for (var c : writes.get(edge.getKey())) {
                        if (a == c || b == c) {
                            continue;
                        }

                        constraintEdges.get(Pair.of(a, c))
                                .add(new SIEdge<>(b, c, EdgeType.RW, edge.getKey()));
                    }
                }
            }
        }

        var constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
        forEachWriteSameKey.accept((a, c, key) -> constraints
                .add(new SIConstraint<>(constraintEdges.get(Pair.of(a, c)),
                        constraintEdges.get(Pair.of(c, a)), a, c)));

        return constraints;
    }

}

@SuppressWarnings("UnstableApiUsage")
class SISolver<KeyType, ValueType> {
    final Solver solver = new Solver();

    // The literals of the known graph
    final Map<Lit, Pair<EndpointPair<Transaction<KeyType, ValueType>>, Collection<Edge<KeyType>>>> knownLiterals = new HashMap<>();

    // The literals asserting that exactly one set of edges exists in the graph
    // for each constraint
    final Map<Lit, SIConstraint<KeyType, ValueType>> constraintLiterals = new HashMap<>();

    boolean solve() {
        var lits = Stream
                .concat(knownLiterals.keySet().stream(),
                        constraintLiterals.keySet().stream())
                .collect(Collectors.toList());

        return solver.solve(lits);
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
            PrecedenceGraph<KeyType, ValueType> precedenceGraph,
            Set<SIConstraint<KeyType, ValueType>> constraints) {
        var profiler = Profiler.getInstance();

        profiler.startTick("SI_SOLVER_GEN_GRAPH_A_B");
        var graphA = createKnownGraph(history,
                precedenceGraph.getKnownGraphA());
        var graphB = createKnownGraph(history,
                precedenceGraph.getKnownGraphB());
        profiler.endTick("SI_SOLVER_GEN_GRAPH_A_B");

        profiler.startTick("SI_SOLVER_GEN_GRAPH_A_UNION_C");
        var matA = new MatrixGraph<>(graphA.asGraph());
        var orderInSession = Utils.getOrderInSession(history);
        var minimalAUnionC = Utils
                .reduceEdges(
                        matA.union(matA.composition("sparse",
                                new MatrixGraph<>(graphB.asGraph()))),
                        orderInSession);
        var reachability = minimalAUnionC.reachability("sparse");

        var knownEdges = Utils.getKnownEdges(graphA, graphB, minimalAUnionC);

        addConstraints(constraints, graphA, graphB);
        var unknownEdges = Utils.getUnknownEdges(graphA, graphB, reachability);
        profiler.endTick("SI_SOLVER_GEN_GRAPH_A_UNION_C");

        List.of(Pair.of('A', graphA), Pair.of('B', graphB)).forEach(p -> {
            var g = p.getRight();
            var edgesSize = g.edges().stream()
                    .map(e -> g.edgeValue(e).get().size()).reduce(Integer::sum)
                    .orElse(0);
            System.err.printf("Graph %s edges count: %d\n", p.getLeft(),
                    edgesSize);
        });
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
            ValueGraph<Transaction<KeyType, ValueType>, Collection<Edge<KeyType>>> knownGraph) {
        var g = Utils.createEmptyGraph(history);
        for (var e : knownGraph.edges()) {
            var lit = new Lit(solver);
            knownLiterals.put(lit, Pair.of(e, knownGraph.edgeValue(e).get()));
            Utils.addEdge(g, e.source(), e.target(), lit);
        }

        return g;
    }

    private void addConstraints(
            Set<SIConstraint<KeyType, ValueType>> constraints,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB) {
        var addEdges = ((Function<List<SIEdge<KeyType, ValueType>>, Pair<Lit, Lit>>) edges -> {
            // all means all edges exists in the graph.
            // Similar for none.
            Lit all = Lit.True, none = Lit.True;
            for (var e : edges) {
                var lit = new Lit(solver);
                all = Logic.and(all, lit);
                none = Logic.and(none, Logic.not(lit));

                if (e.getType().equals(EdgeType.WW)) {
                    Utils.addEdge(graphA, e.from, e.to, lit);
                } else {
                    Utils.addEdge(graphB, e.from, e.to, lit);
                }
            }
            return Pair.of(all, none);
        });

        for (var c : constraints) {
            var p1 = addEdges.apply(c.edges1);
            var p2 = addEdges.apply(c.edges2);

            constraintLiterals
                    .put(Logic.or(Logic.and(p1.getLeft(), p2.getRight()),
                            Logic.and(p2.getLeft(), p1.getRight())), c);
        }
    }
}

@Data
class SIEdge<KeyType, ValueType> {
    final Transaction<KeyType, ValueType> from;
    final Transaction<KeyType, ValueType> to;
    final EdgeType type;
    final KeyType key;

    @Override
    public String toString() {
        return String.format("(%s -> %s, %s, %s)", from, to, type, key);
    }
}

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
class SIConstraint<KeyType, ValueType> {

    // writeTransaction1 -> writeTransaction2
    @ToString.Include(rank = 1)
    final List<SIEdge<KeyType, ValueType>> edges1;

    // writeTransaction2 -> writeTransaction1
    @ToString.Include(rank = 0)
    final List<SIEdge<KeyType, ValueType>> edges2;

    @EqualsAndHashCode.Include
    @ToString.Include(rank = 3)
    final Transaction<KeyType, ValueType> writeTransaction1;

    @EqualsAndHashCode.Include
    @ToString.Include(rank = 2)
    final Transaction<KeyType, ValueType> writeTransaction2;
}

class Utils {
    static <KeyType, ValueType> boolean verifyInternalConsistency(
            History<KeyType, ValueType> history) {
        var writes = new HashMap<Pair<KeyType, ValueType>, Pair<Transaction<KeyType, ValueType>, Integer>>();
        var getEvents = ((Function<Event.EventType, Stream<Pair<Integer, Event<KeyType, ValueType>>>>) type -> history
                .getTransactions().stream().flatMap(txn -> {
                    var events = txn.getEvents();
                    return IntStream.range(0, events.size())
                            .mapToObj(i -> Pair.of(i, events.get(i)))
                            .filter(p -> p.getRight().getType() == type);
                }));

        getEvents.apply(Event.EventType.WRITE).forEach(p -> {
            var i = p.getLeft();
            var ev = p.getRight();
            writes.put(Pair.of(ev.getKey(), ev.getValue()),
                    Pair.of(ev.getTransaction(), i));
        });

        for (var p : getEvents.apply(Event.EventType.READ)
                .collect(Collectors.toList())) {
            var i = p.getLeft();
            var ev = p.getRight();
            var writeEv = writes.get(Pair.of(ev.getKey(), ev.getValue()));

            if (writeEv == null) {
                var txn = ev.getTransaction();
                System.err.printf(
                        "(Session=%s, Transaction=%s, Event=%s) has no corresponding write\n",
                        txn.getSession().getId(), txn.getId(), ev);
                return false;
            }

            if (writeEv.getLeft() == ev.getTransaction()
                    && writeEv.getRight() >= i) {
                var txn = ev.getTransaction();
                System.err.printf(
                        "(%s, %s, %s) reads from a write after it in the same transaction\n",
                        txn.getSession(), txn, ev);
                return false;
            }
        }
        return true;
    }

    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getUnknownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> reachability) {
        var edges = new ArrayList<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>();

        for (var p : graphA.nodes()) {
            for (var n : graphA.successors(p)) {
                var predEdges = graphA.edgeValue(p, n).get();

                if (!reachability.hasEdgeConnecting(p, n)) {
                    predEdges.forEach(e -> edges.add(Triple.of(p, n, e)));
                }

                var txns = graphB.successors(n).stream().filter(
                        t -> !reachability.hasEdgeConnecting(p, t))
                        .collect(Collectors.toList());

                for (var s : txns) {
                    var succEdges = graphB.edgeValue(n, s).get();
                    predEdges.forEach(e1 -> succEdges.forEach(e2 -> {
                        edges.add(Triple.of(p, s, Logic.and(e1, e2)));
                    }));
                }
            }
        }

        return edges;
    }

    static <KeyType, ValueType> List<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>> getKnownEdges(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphA,
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> graphB,
            MatrixGraph<Transaction<KeyType, ValueType>> minimalAUnionC) {
        return minimalAUnionC.edges().stream().map(e -> {
            var n = e.source();
            var m = e.target();
            var firstEdge = ((Function<Optional<Collection<Lit>>, Lit>) c -> c
                    .get().iterator().next());

            if (graphA.hasEdgeConnecting(n, m)) {
                return Triple.of(n, m, firstEdge.apply(graphA.edgeValue(n, m)));
            }

            var middle = Sets
                    .intersection(graphA.successors(n), graphB.predecessors(m))
                    .iterator().next();
            return Triple.of(n, m,
                    Logic.and(firstEdge.apply(graphA.edgeValue(n, middle)),
                            firstEdge.apply(graphB.edgeValue(middle, m))));
        }).collect(Collectors.toList());
    }

    static <KeyType, ValueType> Map<Transaction<KeyType, ValueType>, Integer> getOrderInSession(
            History<KeyType, ValueType> history) {
        // @formatter:off
        return history.getSessions().stream()
                .flatMap(s -> Streams.zip(
                    s.getTransactions().stream(),
                    IntStream.range(0, s.getTransactions().size()).boxed(),
                    Pair::of))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        // @formatter:on
    }

    static <KeyType, ValueType> MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> createEmptyGraph(
            History<KeyType, ValueType> history) {
        MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> g = ValueGraphBuilder
                .directed().allowsSelfLoops(true).build();

        history.getTransactions().forEach(g::addNode);
        return g;
    }

    static <KeyType, ValueType> void addEdge(
            MutableValueGraph<Transaction<KeyType, ValueType>, Collection<Lit>> g,
            Transaction<KeyType, ValueType> src,
            Transaction<KeyType, ValueType> dst, Lit lit) {
        if (!g.hasEdgeConnecting(src, dst)) {
            g.putEdgeValue(src, dst, new ArrayList<>());
        }
        g.edgeValue(src, dst).get().add(lit);
    }

    static <KeyType, ValueType> MatrixGraph<Transaction<KeyType, ValueType>> reduceEdges(
            MatrixGraph<Transaction<KeyType, ValueType>> graph,
            Map<Transaction<KeyType, ValueType>, Integer> orderInSession) {
        System.err.printf("Before: %d edges\n", graph.edges().size());
        var newGraph = MatrixGraph.ofNodes(graph);

        for (var n : graph.nodes()) {
            var succ = graph.successors(n);
            // @formatter:off
            var firstInSession = succ.stream()
                .collect(Collectors.toMap(
                    m -> m.getSession(),
                    Function.identity(),
                    (p, q) -> orderInSession.get(p)
                        < orderInSession.get(q) ? p : q));

            firstInSession.values().forEach(m -> newGraph.putEdge(n, m));

            succ.stream()
                .filter(m -> m.getSession() == n.getSession()
                        && orderInSession.get(m) == orderInSession.get(n) + 1)
                .forEach(m -> newGraph.putEdge(n, m));
            // @formatter:on
        }

        System.err.printf("After: %d edges\n", newGraph.edges().size());
        return newGraph;
    }
}
