package verifier;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.HashBiMap;
import com.google.common.graph.*;

import graph.History;
import graph.HistoryLoader;
import graph.PrecedenceGraph2;
import graph.History.Transaction;
import kvClient.pb.Key;
import lombok.Data;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.units.qual.K;
import util.QuadConsumer;
import util.TriConsumer;

@SuppressWarnings("UnstableApiUsage")
public class SIVerifier<KeyType, ValueType> {
	private final History<KeyType, ValueType> history;

	public SIVerifier(HistoryLoader<KeyType, ValueType> loader) {
		history = loader.loadHistory();
		System.err.printf("Sessions count: %d\nTransactions count: %d\nEvents count: %d\n",
			history.getSessions().size(),
			history.getTransactions().size(),
			history.getEvents().size());
	}

	public boolean audit() {
		if (!verifyInternalConsistency(history)) {
			return false;
		}

		var graph = new PrecedenceGraph2<>(history);
		System.err.printf("WR edges count: %d\nSO edges count: %d\n",
			graph.getReadFrom().edges().stream()
				.map(e -> graph.getReadFrom().edgeValue(e).get().size())
				.reduce(Integer::sum).get(),
			graph.getSessionOrder().edges().size());

		var constraints = generateConstraints(history, graph);
		System.err.printf("Constraints count: %d\n\n", constraints.size());

		var solver = new SISolver<>(history, graph, constraints);
		return solver.solve();
	}

	/*
	 * Generate constraints from a precedence graph
	 *
	 * @param graph the graph to use
	 *
	 * @return the set of constraints generated
	 *
	 * For each triple of transactions A, B, C such that B reads A and C writes the
	 * key B read, generate polygraph for the following two conditions:
	 *
	 * 1. C precedes A, then C ->(ww) A ->(wr) B
	 *
	 * 2. A precedes C, then A ->(wr) B ->(rw) C, A ->(ww) C
	 *
	 * Furthermore, for each key and each pair of transactions A, B that have
	 * written to this key, generate a constraint for them containing WW edges.
	 *
	 * Note that it is possible for C to precede B in SI.
	 */
	private Set<SIConstraint<KeyType, ValueType>> generateConstraints(
		History<KeyType, ValueType> history,
		PrecedenceGraph2<KeyType, ValueType> graph) {
		var readFrom = graph.getReadFrom();
		var constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
		var writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

		history.getEvents().stream()
			.filter(e -> e.getType() == History.EventType.WRITE)
			.forEach(ev -> {
				writes.computeIfAbsent(ev.getKey(), k -> new HashSet<>()).add(ev.getTransaction());
			});

		for (var a : history.getTransactions()) {
			for (var b : readFrom.successors(a)) {
				for (var key: readFrom.edgeValue(a, b).get()) {
					for (var c : writes.get(key)) {
						if (a == c || b == c) {
							continue;
						}

						constraints.add(new SIConstraint<>(
							List.of(new SIEdge<>(c, a, EdgeType.WW)),
							List.of(new SIEdge<>(b, c, EdgeType.RW), new SIEdge<>(a, c, EdgeType.WW))
						));
					}
				}
			}
		}

		for (var txns: writes.values()) {
			var list = new ArrayList<>(txns);

			for (var i=0 ; i< list.size();i++) {
				for (var j=i+1;j< list.size();j++) {
					constraints.add(new SIConstraint<>(
						List.of(new SIEdge<>(list.get(i), list.get(j), EdgeType.WW)),
						List.of(new SIEdge<>(list.get(j), list.get(i), EdgeType.WW))
					));
				}
			}
		}
		return constraints;
	}

	boolean verifyInternalConsistency(History<KeyType, ValueType> history) {
		var writes = new HashMap<Pair<KeyType, ValueType>, Pair<Transaction<KeyType, ValueType>, Integer>>();
		Function<History.EventType, Stream<Pair<Integer, History.Event<KeyType, ValueType>>>> getEvents =
			type -> history.getTransactions().stream()
				.flatMap(txn -> {
					var events = txn.getEvents();
					return IntStream.range(0, events.size())
						.mapToObj(i -> Pair.of(i, events.get(i)))
						.filter(p -> p.getRight().getType() == type);
				});

		getEvents.apply(History.EventType.WRITE).forEach(p -> {
			var i = p.getLeft();
			var ev = p.getRight();
			writes.put(Pair.of(ev.getKey(), ev.getValue()),
				Pair.of(ev.getTransaction(), i));
		});

		for (var p: getEvents.apply(History.EventType.READ).collect(Collectors.toList())) {
			var i = p.getLeft();
			var ev = p.getRight();
			var writeEv = writes.get(Pair.of(ev.getKey(), ev.getValue()));

			if (writeEv.getLeft() == ev.getTransaction()
				&& writeEv.getRight() >= i) {
				return false;
			}
		}
		return true;
	}
}

@SuppressWarnings("UnstableApiUsage")
class SISolver<KeyType, ValueType> {
	final Solver solver = new Solver();

	// The literals of WR and SO edges
	final Map<Lit, EndpointPair<Transaction<KeyType, ValueType>>> edgeLiterals = new HashMap<>();

	// The literals asserting that exactly one set of edges exists in the graph
	// for each constraint
	final Map<Lit, SIConstraint<KeyType, ValueType>> constraintLiterals = new HashMap<>();

	boolean solve() {
		var lits = Stream.concat(
			edgeLiterals.keySet().stream(),
			constraintLiterals.keySet().stream()).collect(Collectors.toList());

		if (solver.solve(lits)) {
			return true;
		} else {
			System.err.println("Conflicts:");
			for (var lit: solver.getConflictClause().stream()
				.map(Logic::not).collect(Collectors.toList())) {
				if (edgeLiterals.containsKey(lit)) {
					System.err.printf("Edge: %s\n", edgeLiterals.get(lit));
				} else {
					System.err.printf("Constraint: %s\n", constraintLiterals.get(lit));
				}
			}
			return false;
		}
	}

	/*
	 * Construct SISolver from constraints
	 *
	 * First construct two graphs:
	 * 1. Graph A contains WR, WW and SO edges.
	 * 2. Graph B contains RW edges.
	 *
	 * For each edge in A and B, create a literal for it. The edge exists in
	 * the final graph iff. the literal is true.
	 *
	 * Then, construct a third graph C using A and B: If P -> Q in A and Q -> R
	 * in B, then P -> R in C The literal of P -> R is ((P -> Q) and (Q -> R)).
	 *
	 * Lastly, we add graph A and C to monosat, resulting in the final graph.
	 *
	 * Literals that are passed as assumptions to monograph:
	 * 1. The literals of WR, SO edges, because those edges always exist.
	 * 2. For each constraint, a literal that asserts exactly one set of edges
	 *    exist in the graph.
	 */
	SISolver(History<KeyType, ValueType> history,
			 PrecedenceGraph2<KeyType, ValueType> precedenceGraph,
			 Set<SIConstraint<KeyType, ValueType>> constraints) {

		var monoGraph = new monosat.Graph(solver);
		var nodeMap = new HashMap<Transaction<KeyType, ValueType>, Integer>();
		var graphA = createWRAndSOGraph(history, precedenceGraph);
		var graphB = createEmptyGraph(history);

		history.getTransactions().forEach(n -> {
			nodeMap.put(n, monoGraph.addNode());
		});

		addConstraints(constraints, graphA, graphB);
		var graphC = composition(history, graphA, graphB);

		var addGraph = ((Consumer<ValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>>) g -> {
			for (var n : g.nodes()) {
				for (var s : g.successors(n)) {
					for (var e : g.edgeValue(n, s).orElse(Set.of())) {
						solver.assertEqual(e, monoGraph.addEdge(nodeMap.get(n), nodeMap.get(s)));
					}
				}
			}
		});

		addGraph.accept(graphA);
		addGraph.accept(graphC);

		solver.assertTrue(monoGraph.acyclic());
	}

	private MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>
	createWRAndSOGraph(History<KeyType, ValueType> history,
					   PrecedenceGraph2<KeyType, ValueType> precedenceGraph) {
		var graphA = createEmptyGraph(history);

		Consumer<Graph<Transaction<KeyType, ValueType>>> addToGraphA = (graph -> {
			for (var e: graph.edges()) {
				var lit = new Lit(solver);
				edgeLiterals.put(lit, e);
				addEdge(graphA, e.source(), e.target(), lit);
			}
		});

		addToGraphA.accept(precedenceGraph.getReadFrom().asGraph());
		addToGraphA.accept(precedenceGraph.getSessionOrder());
		return graphA;
	}

	private void
	addConstraints(Set<SIConstraint<KeyType, ValueType>> constraints,
				   MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>> graphA,
				   MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>> graphB) {
		var addEdges = ((Function<List<SIEdge<KeyType, ValueType>>, Pair<Lit, Lit>>) edges -> {
			// all means all edges exists in the graph.
			// Similar for none.
			Lit all = Lit.True, none = Lit.True;
			for (var e: edges) {
				var lit = new Lit(solver);
				all = Logic.and(all, lit);
				none = Logic.and(none, Logic.not(lit));

				if (e.getType().equals(EdgeType.WW)) {
					addEdge(graphA, e.from, e.to, lit);
				} else {
					addEdge(graphB, e.from, e.to, lit);
				}
			}
			return Pair.of(all, none);
		});

		for (var c: constraints) {
			var p1 = addEdges.apply(c.edges1);
			var p2 = addEdges.apply(c.edges2);

			constraintLiterals.put(
				Logic.or(
					Logic.and(p1.getLeft(), p2.getRight()),
					Logic.and(p2.getLeft(), p1.getRight())),
				c);
		}
	}

	private MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>
	createEmptyGraph(History<KeyType, ValueType> history) {
		MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>> g =
			ValueGraphBuilder.directed().allowsSelfLoops(true).build();

		history.getTransactions().forEach(g::addNode);
		return g;
	}

	private MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>
	composition(History<KeyType, ValueType> history,
				MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>> graphA,
				MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>> graphB) {
		var graphC = createEmptyGraph(history);

		for (var n: history.getTransactions()) {
			var pred = graphA.predecessors(n);
			var succ = graphB.successors(n);

			for (var p: pred) {
				for (var s: succ) {
					var predEdges = graphA.edgeValue(p, n).orElse(Set.of());
					var succEdges = graphB.edgeValue(n, s).orElse(Set.of());

					predEdges.forEach(e1 -> succEdges.forEach(e2 -> {
						addEdge(graphC, p, s, Logic.and(e1, e2));
					}));
				}
			}
		}
		return graphC;
	}

	private void addEdge(
		MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>> g,
		Transaction<KeyType, ValueType> src,
		Transaction<KeyType, ValueType> dst,
		Lit lit) {
		if (!g.hasEdgeConnecting(src, dst)) {
			g.putEdgeValue(src, dst, new HashSet<>());
		}
		g.edgeValue(src, dst).get().add(lit);
	};
}

enum EdgeType {
	WW, RW
}

@Data
class SIEdge<KeyType, ValueType> {
	final Transaction<KeyType, ValueType> from;
	final Transaction<KeyType, ValueType> to;
	final EdgeType type;
}

@Data
class SIConstraint<KeyType, ValueType> {
	final List<SIEdge<KeyType, ValueType>> edges1;
	final List<SIEdge<KeyType, ValueType>> edges2;
}
