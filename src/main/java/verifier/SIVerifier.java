package verifier;

import java.util.*;

import graph.PrecedenceGraph;
import graph.TxnNode;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import util.Pair;
import util.UnimplementedError;
import util.VeriConstants;

public class SIVerifier extends AbstractLogVerifier {
	// precedence graph constructed from logs
	private final PrecedenceGraph graph;

	public SIVerifier(String logfd) {
		super(logfd);
		graph = createKnownGraph();
	}

	@Override
	public boolean audit() {
		var constraints = generateConstraints(graph);
		throw new UnimplementedError();
	}

	@Override
	public int[] count() {
		throw new Error("Not supported");
	}

	@Override
	public boolean continueslyAudit() {
		throw new Error("Not supported");
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
	 * For each triple of transactions A, B, C
	 * such that B reads A and C writes the key B read,
	 * generate polygraph for the following two conditions:
	 *
	 * 1. C precedes A, then C ->(ww) A ->(wr) B
	 * 2. A precedes C, then A ->(wr) B ->(rw) C, A ->(ww) C
	 *
	 * Note that it is possible for C to precede B in SI.
	 */
	private static Set<SIConstraint> generateConstraints(PrecedenceGraph graph) {
		var writeTxns = getKeyAndWriteTxns(graph);
		var readOps = graph.m_readFromMapping;
		var constraints = new HashSet<SIConstraint>();

		for (var a: graph.allNodes()) {
			for (var op: a.getOps()) {
				if (op.isRead) {
					continue;
				}

				for (var bop: readOps.get(op.id)) {
					var b = graph.getNode(bop.txnid);
					for (var c: writeTxns.get(op.key_hash)) {
						constraints.add(new SIConstraint(
							new SIEdge[]{
								new SIEdge(c, a, EdgeType.WW),
								new SIEdge(a, b, EdgeType.WR)
							},
							new SIEdge[]{
								new SIEdge(a, b, EdgeType.WR),
								new SIEdge(b, c, EdgeType.RW),
								new SIEdge(a, c, EdgeType.WW)
							}
						));
					}
				}
			}
		}

		return constraints;
	}

	// get the set of txns that write to each key
	private static Map<Long, Set<TxnNode>> getKeyAndWriteTxns(PrecedenceGraph graph) {
		var m = new HashMap<Long, Set<TxnNode>>();
		for (var t: graph.allNodes()) {
			for (var op: t.getOps()) {
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

enum EdgeType {
	WW, WR, RW
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
