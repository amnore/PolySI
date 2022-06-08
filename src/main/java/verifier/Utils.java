package verifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import graph.MatrixGraph;
import history.Event;
import history.History;
import history.Transaction;
import history.Event.EventType;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;

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
            MatrixGraph<Transaction<KeyType, ValueType>> reachability,
            Solver solver) {
        var edges = new ArrayList<Triple<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>, Lit>>();

        for (var p : graphA.nodes()) {
            for (var n : graphA.successors(p)) {
                var predEdges = graphA.edgeValue(p, n).get();

                if (!reachability.hasEdgeConnecting(p, n)) {
                    predEdges.forEach(e -> edges.add(Triple.of(p, n, e)));
                }

                var txns = graphB.successors(n).stream()
                        .filter(t -> !reachability.hasEdgeConnecting(p, t))
                        .collect(Collectors.toList());

                for (var s : txns) {
                    var succEdges = graphB.edgeValue(n, s).get();
                    predEdges.forEach(e1 -> succEdges.forEach(e2 -> {
                        var lit = Logic.and(e1, e2);
                        solver.setDecisionLiteral(lit, false);
                        edges.add(Triple.of(p, s, lit));
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
