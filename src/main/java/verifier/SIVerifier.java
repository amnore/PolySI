package verifier;

import java.util.HashSet;
import java.util.Set;

import graph.PrecedenceGraph;
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
	private static Set<Constraint> generateConstraints(PrecedenceGraph graph) {
		for (var b: graph.allEdges()) {
		}

		throw new UnimplementedError();
	}
}
