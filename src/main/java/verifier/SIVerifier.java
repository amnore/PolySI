package verifier;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.HashBiMap;
import com.google.common.graph.*;

import graph.History;
import graph.HistoryLoader;
import graph.PrecedenceGraph2;
import graph.History.Transaction;
import io.grpc.netty.shaded.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;
import lombok.Data;
import lombok.extern.java.Log;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.units.qual.K;
import util.QuadConsumer;

@SuppressWarnings("UnstableApiUsage")
public class SIVerifier<KeyType, ValueType> {
	private final History<KeyType, ValueType> history;
	private final PrecedenceGraph2<KeyType, ValueType> graph;

	public SIVerifier(HistoryLoader<KeyType, ValueType> loader) {
		history = loader.loadHistory();
		graph = new PrecedenceGraph2<>(history);
		System.err.printf("Number of WR edges: %d\nSO edges: %d\n",
			graph.getReadFrom().edges().stream().map(e -> graph.getReadFrom().edgeValue(e).get().size()).reduce(Integer::sum).get(),
			graph.getSessionOrder().edges().size());
	}

	public boolean audit() {
		var constraints = generateConstraints();
		System.err.printf("Number of constraints: %d\n\n", constraints.size());
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
	private Set<SIConstraint<KeyType, ValueType>> generateConstraints() {
		var readFrom = graph.getReadFrom();
		var constraints = new HashSet<SIConstraint<KeyType, ValueType>>();
		var writes = new HashMap<KeyType, Set<Transaction<KeyType, ValueType>>>();

		history.getEvents().stream().filter(e -> e.getType() == History.EventType.WRITE)
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
}

@SuppressWarnings("UnstableApiUsage")
class SISolver<KeyType, ValueType> {
	final Solver solver = new Solver();
	final monosat.Graph graph = new monosat.Graph(solver);
	final HashBiMap<Transaction<KeyType, ValueType>, Integer> nodeMap = HashBiMap.create();
	final HashMap<Lit, Pair<Transaction<KeyType, ValueType>, Transaction<KeyType, ValueType>>> litEdges = new HashMap<>();

	/*
	 * Construct SISolver from constraints
	 *
	 * First construct two graphs:
	 *
	 * 1. Graph A contains WR, WW and SO edges.
	 *
	 * 2. Graph B contains RW edges.
	 *
	 * Each edge is associated with a literal. The edge exists in the graph iff. the
	 * literal is true.
	 *
	 * 1. For WR edges, the literal is asserted to be true
	 *
	 * 2. For WW and RW edges, the literal is created from the constraints. For edge
	 * sets S1 and S2 in a same constraint, the literal of S1 is the negation of
	 * S2's.
	 *
	 * Then, we construct a third graph C using these two: If P -> Q in A and Q -> R
	 * in B, then P -> R in C The literal of P -> R is the conjunction of their
	 * literals.
	 *
	 * Lastly, we add graph A and C to monosat and assert this graph is acyclic.
	 */
	SISolver(History<KeyType, ValueType> history,
			 PrecedenceGraph2<KeyType, ValueType> precedenceGraph,
			 Set<SIConstraint<KeyType, ValueType>> constraints) {
		Supplier<MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>> newGraph =
			() -> ValueGraphBuilder.directed()
				.allowsSelfLoops(true)
				.build();
		QuadConsumer<MutableValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>,
			Transaction<KeyType, ValueType>,
			Transaction<KeyType, ValueType>,
			Lit> addEdge = (g, src, dst, lit) -> {
			if (!g.hasEdgeConnecting(src, dst)) {
				g.putEdgeValue(src, dst, new HashSet<>());
			}
			g.edgeValue(src, dst).get().add(lit);
		};

		var graphA = newGraph.get();
		var graphB = newGraph.get();
		var graphC = newGraph.get();

		history.getTransactions().forEach(n -> {
			graphA.addNode(n);
			graphB.addNode(n);
			graphC.addNode(n);
			nodeMap.put(n, graph.addNode());
		});

		Consumer<Graph<Transaction<KeyType, ValueType>>> addToGraphA = (graph -> {
			for (var e: graph.edges()) {
				var lit = new Lit(solver);
				solver.assertTrue(lit);
				addEdge.accept(graphA, e.source(), e.target(), lit);
			}
		});

		// add WR and SO edges
		addToGraphA.accept(precedenceGraph.getReadFrom().asGraph());
		addToGraphA.accept(precedenceGraph.getSessionOrder());

		// add WW and RW edges
		constraints.forEach(c -> {
			BiConsumer<Lit, List<SIEdge<KeyType, ValueType>>> addEdges = (lit, edges) -> {
				edges.forEach(e -> {
					var graph = e.type == EdgeType.WW ? graphA : graphB;
					addEdge.accept(graph, e.from, e.to, lit);
				});
			};

			var lit = new Lit(solver);
			addEdges.accept(lit, c.edges1);
			addEdges.accept(Logic.not(lit), c.edges2);
		});

		// construct graphC
		for (var n: history.getTransactions()) {
			var pred = graphA.predecessors(n);
			var succ = graphB.successors(n);

			for (var p: pred) {
				for (var s: succ) {
					var predEdges = graphA.edgeValue(p, n).orElse(Set.of());
					var succEdges = graphB.edgeValue(n, s).orElse(Set.of());

					predEdges.forEach(e1 -> succEdges.forEach(e2 -> {
						addEdge.accept(graphC, p, s, Logic.and(e1, e2));
					}));
				}
			}
		}

		Consumer<ValueGraph<Transaction<KeyType, ValueType>, Set<Lit>>> addGraph = g -> {
			for (var n : g.nodes()) {
				for (var s : g.successors(n)) {
					for (var e : g.edgeValue(n, s).orElse(Set.of())) {
						litEdges.put(e, Pair.of(n, s));
						solver.assertEqual(e, graph.addEdge(nodeMap.get(n), nodeMap.get(s)));
					}
				}
			}
		};

		addGraph.accept(graphA);
		addGraph.accept(graphC);

		solver.assertTrue(graph.acyclic());
	}

	boolean solve() {
		var ret = solver.solve();
		return ret;
	}
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
