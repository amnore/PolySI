package verifier;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import graph.PrecedenceGraph;
import graph.TxnNode;
import io.grpc.netty.shaded.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import monosat.Graph;
import monosat.Lit;
import monosat.Logic;
import monosat.Solver;
import util.UnimplementedError;
import util.VeriConstants;

public class SIVerifier extends AbstractLogVerifier {
	// precedence graph constructed from logs
	final PrecedenceGraph graph;

	public SIVerifier(String logfd) {
		super(logfd);
		graph = createKnownGraph();
	}

	@Override
	public boolean audit() {
		var constraints = generateConstraints(graph);
		var solver = new SISolver(graph, constraints);
		return solver.solve();
	}

	@Override
	public int[] count() {
		throw new UnimplementedError();
	}

	@Override
	public boolean continueslyAudit() {
		throw new UnimplementedError();
	}

	/*
	 * Create precedence graph from logs
	 *
	 * @return the created graph
	 */
	private PrecedenceGraph createKnownGraph() {
		var files = findOpLogInDir(log_dir);

		// create an initial graph
		var graph = new PrecedenceGraph();
		graph.addTxnNode(new graph.TxnNode(VeriConstants.INIT_TXN_ID));

		// load nodes from logs into graph
		if (!loadLogs(files, graph)) {
			throw new Error("load log failed");
		}

		if (!graph.isComplete() || !graph.readWriteMatches()) {
			throw new Error("Malformed graph");
		}

		System.err.printf("#clients=%d\n", client_list.size());
		System.err.printf("#graph nodes=%d\n", graph.allNodes().size());

		return graph;
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
	 * Note that it is possible for C to precede B in SI.
	 */
	private static Set<SIConstraint> generateConstraints(PrecedenceGraph graph) {
		var writeTxns = getKeyAndWriteTxns(graph);
		var readOps = graph.m_readFromMapping;
		var constraints = new HashSet<SIConstraint>();

		for (var a : graph.allNodes()) {
			for (var op : a.getOps()) {
				if (op.isRead) {
					continue;
				}

				for (var bop : readOps.getOrDefault(op.id, new HashSet<>())) {
					var b = graph.getNode(bop.txnid);
					for (var c : writeTxns.get(op.key_hash)) {
						if (a == c) {
							continue;
						}

						constraints.add(new SIConstraint(new SIEdge[] { new SIEdge(c, a, EdgeType.WW), },
								new SIEdge[] { new SIEdge(b, c, EdgeType.RW), new SIEdge(a, c, EdgeType.WW) }));
					}
				}
			}
		}

		return constraints;
	}

	// get the set of txns that write to each key
	private static Map<Long, Set<TxnNode>> getKeyAndWriteTxns(PrecedenceGraph graph) {
		var m = new HashMap<Long, Set<TxnNode>>();
		for (var t : graph.allNodes()) {
			for (var op : t.getOps()) {
				if (op.isRead) {
					continue;
				}

				if (!m.containsKey(op.key_hash)) {
					m.put(op.key_hash, new HashSet<>());
				}
				m.get(op.key_hash).add(t);
			}
		}

		return m;
	}
}

class SISolver {
	final Solver solver = new Solver();
	final Graph graph = new Graph(solver);
	final HashSet<Lit> lits = new HashSet<>();
	final HashBiMap<TxnNode, Integer> nodeMap = HashBiMap.create();

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
	SISolver(PrecedenceGraph precedenceGraph, Set<SIConstraint> constraints) {
		Supplier<MutableNetwork<TxnNode, Lit>> newGraph = () -> NetworkBuilder.directed().allowsParallelEdges(true)
				.build();
		var graphA = newGraph.get();
		var graphB = newGraph.get();
		var graphC = newGraph.get();

		precedenceGraph.allNodes().forEach(n -> {
			graphA.addNode(n);
			graphB.addNode(n);
			graphC.addNode(n);
			nodeMap.put(n, graph.addNode());
		});

		// add WR edges
		precedenceGraph.allEdges().forEach(e -> {
			var lit = new Lit(solver);
			solver.assertTrue(lit);
			graphA.addEdge(e.source(), e.target(), lit);
		});

		// add SO edges
		precedenceGraph.allNodes().stream().filter(n -> n.id != VeriConstants.INIT_TXN_ID).forEach(n -> {
			var lit = new Lit(solver);
			solver.assertTrue(lit);
			var prevId = n.getPreviousClientTxn() == VeriConstants.NULL_TXN_ID ? VeriConstants.INIT_TXN_ID : n.getPreviousClientTxn();
			graphA.addEdge(precedenceGraph.getNode(prevId), n, lit);
		});

		// add WW and RW edges
		constraints.forEach(c -> {
			BiConsumer<Lit, SIEdge[]> addEdges = (lit, edges) -> {
				Arrays.stream(edges).forEach(e -> {
					var graph = e.type == EdgeType.WW ? graphA : graphB;
					graph.addEdge(e.from, e.to, lit);
				});
			};

			var lit = new Lit(solver);
			addEdges.accept(lit, c.edges1);
			addEdges.accept(Logic.not(lit), c.edges2);
		});

		// construct graphC
		precedenceGraph.allNodes().forEach(n -> {
			var pred = graphA.predecessors(n);
			var succ = graphB.successors(n);

			pred.forEach(p -> succ.forEach(s -> {
				var predEdges = graphA.edgesConnecting(p, n);
				var succEdges = graphA.edgesConnecting(n, s);

				predEdges.forEach(e1 -> succEdges.forEach(e2 -> graphC.addEdge(p, s, Logic.and(e1, e2))));
			}));
		});

		Consumer<Network<TxnNode, Lit>> addGraph = g -> {
			for (var n : g.nodes()) {
				for (var s : g.successors(n)) {
					for (var e : g.edgesConnecting(n, s)) {
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
		return solver.solve();
	}
}

enum EdgeType {
	WW, RW
}

@Data
class SIEdge {
	final TxnNode from;
	final TxnNode to;
	final EdgeType type;
}

@Data
class SIConstraint {
	final SIEdge[] edges1;
	final SIEdge[] edges2;
}
