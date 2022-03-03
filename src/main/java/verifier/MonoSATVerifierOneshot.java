package verifier;

import gpu.ReachabilityMatrix;
import graph.EdgeType;
import graph.OpNode;
import graph.PrecedenceGraph;
import graph.TxnNode;
import graph.TxnNode.TxnType;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import algo.DFSCycleDetection;
import algo.TarjanStronglyConnectedComponents;

import com.google.common.graph.EndpointPair;

import util.ChengLogger;
import util.Pair;
import util.Profiler;
import util.VeriConstants;
import util.VeriConstants.LoggerType;

public class MonoSATVerifierOneshot extends AbstractLogVerifier {

	public MonoSATVerifierOneshot(String logfd) {
	  super(logfd);
  }

	// ==============================
	// ===== main logic =============
  // ==============================

	protected Set<Constraint> GenConstraints(PrecedenceGraph g) {
		Map<Long, Set<List<TxnNode>>> chains = new HashMap<Long, Set<List<TxnNode>>>(); // key -> Set<List>
		Map<Long,Long> wid2txnid = new HashMap<Long,Long>();
		Map<Long,Long> wid2key = new HashMap<Long,Long>();

		// each key maps to set of chains; each chain is an ordered list
		for (TxnNode tx : g.allNodes()) {
			for (OpNode op : tx.getOps()) {
				if (!op.isRead) {
					Long key = op.key_hash;
					if (!chains.containsKey(key)) {
						chains.put(key, new HashSet<List<TxnNode>>());
					}
					List<TxnNode> singleton_list = new ArrayList<TxnNode>();
					singleton_list.add(tx);
					chains.get(key).add(singleton_list);
					// wid => txnid
					assert !wid2txnid.containsKey(op.wid);
					wid2txnid.put(op.wid, op.txnid);
					wid2key.put(op.wid, op.key_hash);
				}
			}
		}

		// construct the constraints
		Set<Constraint> cons = new HashSet<Constraint>();
		for (Long key : chains.keySet()) {
			// skip fence txs
			if (key == VeriConstants.VERSION_KEY_HASH) {continue;}

			// take care of init tx, that chain happens before all the other chains
			List<TxnNode> init_chain = null;
			for (List<TxnNode> chain : chains.get(key)) {
				if (chain.get(0).getTxnid() == VeriConstants.INIT_TXN_ID) {
					init_chain = chain;
				}
			}
			if (init_chain != null) {
				for (List<TxnNode> chain : chains.get(key)) {
					if (chain != init_chain) {
						g.addEdge(init_chain.get(init_chain.size()-1), chain.get(0), EdgeType.CONS_SOLV);
					}
				}
			}

			// create a constraint for each pair of chains
			List<List<TxnNode>> chainset = new ArrayList<List<TxnNode>>(chains.get(key));
			// tag whether a chain is frozen (frozen if all the txs are frozen)
			Map<Integer, Boolean> chain_frozen = new HashMap<Integer,Boolean>();
			for (int i=0; i<chainset.size(); i++) {
				boolean frozen = true;
				for (TxnNode tx : chainset.get(i)) {
					if (!tx.frozen) {
						frozen = false; break;
					}
				}
				chain_frozen.put(i, frozen);
			}

			// generate constraints from a pair of chains
			int len = chainset.size();
			for (int i = 0; i < len; i++) {
				for (int j = i + 1; j < len; j++) {
					// if both are frozen; we can skip this (because they've been tested before)
					if (chain_frozen.get(i) && chain_frozen.get(j)) {
						continue;
					}
					// UTBABUG: if chain[i] ~-> chain[j], it doesn't mean we can skip the constraint,
					// because the reachabilities we know might not include what read-txns  should be placed!
					List<Constraint> con_lst = NoCoalesce(chainset.get(i), chainset.get(j), key, g.m_readFromMapping, wid2txnid);
					cons.addAll(con_lst);
				}
			}
		}

		return cons;
	}

	private List<Constraint> NoCoalesce(List<TxnNode> chain_1, List<TxnNode> chain_2, Long key,
			Map<Long, Set<OpNode>> readfrom, Map<Long, Long> wid2txnid)
	{
		ArrayList<Constraint> ret = new ArrayList<Constraint>();
		long head_1 = chain_1.get(0).getTxnid();
		long head_2 = chain_2.get(0).getTxnid();
		long tail_1 = chain_1.get(chain_1.size()-1).getTxnid();
		long tail_2 = chain_2.get(chain_2.size()-1).getTxnid();
		// (1) construct constraints: <read_from_tail_1 -> head_2, tail_2 -> head_1>
		Set<Pair<Long, Long>> edge_set1 = GenChainToChainEdge(chain_1, chain_2, key, readfrom, wid2txnid);
		Pair<Long,Long> sec_edge1 = new Pair<Long,Long>(tail_2, head_1);
		for (Pair<Long, Long> e : edge_set1) {
			Set<Pair<Long,Long>> set1 = new HashSet<Pair<Long,Long>>();
			set1.add(e);
			Set<Pair<Long,Long>> set2 = new HashSet<Pair<Long,Long>>();
			set2.add(sec_edge1);
			ret.add(new Constraint(set1, set2, chain_1, chain_2));
		}

	  // (2) construct constraints: <read_from_tail_2 -> head_1, tail_1 -> head_2>
		Set<Pair<Long,Long>> edge_set2 = GenChainToChainEdge(chain_2, chain_1, key, readfrom, wid2txnid);
		Pair<Long,Long> sec_edge2 = new Pair<Long,Long>(tail_1, head_2);
		for (Pair<Long, Long> e :edge_set2) {
			Set<Pair<Long,Long>> set1 = new HashSet<Pair<Long,Long>>();
			set1.add(e);
			Set<Pair<Long,Long>> set2 = new HashSet<Pair<Long,Long>>();
			set2.add(sec_edge2);
			ret.add(new Constraint(set1, set2, chain_2, chain_1));
		}
		return ret;
	}

	private Set<Pair<Long, Long>> GenChainToChainEdge(List<TxnNode> chain_1, List<TxnNode> chain_2, Long key,
			Map<Long, Set<OpNode>> readfrom, Map<Long, Long> wid2txnid) {
		Long tail_1_wid = null;
		for (OpNode op : chain_1.get(chain_1.size()-1).getOps()) {
			if (!op.isRead && op.key_hash == key) {
				tail_1_wid = op.wid;
			}
		}
		assert tail_1_wid != null;

		long tail_1 = chain_1.get(chain_1.size()-1).getTxnid();
		long head_2 = chain_2.get(0).getTxnid();

		Set<Pair<Long,Long>> ret = new HashSet<Pair<Long,Long>>();
		if (!readfrom.containsKey(tail_1_wid)) {
			assert tail_1 != head_2;
			ret.add(new Pair<Long,Long>(tail_1, head_2));
			return ret;
		}

		assert readfrom.get(tail_1_wid).size() > 0;
		for (OpNode op : readfrom.get(tail_1_wid)) {
			long rtx = op.txnid;
			// it is possible (without WW_CONSTRAINTS) that a reading-from transaction (rtx) and
			// another write (head_2) are the same txn (a successive write)
			if (rtx != head_2) {
				ret.add(new Pair<Long,Long>(rtx, head_2));
			}
		}
		return ret;
	}

	// =========================
	// ===== SMT Solver =========
	// =========================

	public static boolean solveConstraints(PrecedenceGraph g, Set<Constraint> cons, Set<Pair<Long,Long>> solution) {
		SMTEncoderSOSP encoder = new SMTEncoderSOSP(g, cons);
		//SMTEncoder encoder = new SMTEncoder(g, cons);
		boolean ret = encoder.Solve();
		if (!ret) {
			// dump the failed transactions
			ArrayList<EndpointPair<TxnNode>> out_edges = new ArrayList<EndpointPair<TxnNode>>();
			ArrayList<Constraint> out_cons = new ArrayList<Constraint>();
			encoder.GetMinUnsatCore(out_edges, out_cons);

			// print out
			ChengLogger.println(LoggerType.ERROR, "========= MiniUnsatCore ============");
			ChengLogger.println(LoggerType.ERROR, "  === 1. edges ===");
			Set<TxnNode> unsat_txns = new HashSet<TxnNode>();
			for (EndpointPair<TxnNode> e : out_edges) {
				ChengLogger.println(LoggerType.ERROR, "  " + e.source().toString3() + "\n    -> " + e.target().toString3());
				unsat_txns.add(e.source());
				unsat_txns.add(e.target());
			}
			ChengLogger.println(LoggerType.ERROR, "  === 2. constraints ===");
			for (Constraint c : out_cons) {
				ChengLogger.println(LoggerType.ERROR, "  " + c.toString(g));
				unsat_txns.addAll(c.chain_1);
				unsat_txns.addAll(c.chain_2);
			}
			ChengLogger.println(LoggerType.ERROR, "  === 3. transaction details ===");
			for (TxnNode t : unsat_txns) {
				ChengLogger.println(LoggerType.ERROR, "  " + t.toString2());
			}
		}

		if (ret && solution != null) { // if we have a solution
			solution.addAll(encoder.GetSolutionEdges());
		}


		return ret;
	}

	// =========================
	// ===== main APIs =========
	// =========================

	protected PrecedenceGraph CreateKnownGraph() {
		ArrayList<File> opfiles = findOpLogInDir(log_dir);
		boolean ret = loadLogs(opfiles, m_g);
		CheckValues(m_g); // check whether all the read/write values match
		if (!ret) assert false; // Reject

		// check incomplete
		boolean pass = m_g.isComplete();
		if (!pass) {
			ChengLogger.println(LoggerType.ERROR, "REJECT: The history is incomplete!");
			assert false;
		}

		ChengLogger.println("[1] #Clients=" + this.client_list.size());
		ChengLogger.println("[1] global graph: #n=" + m_g.allNodes().size());
		System.err.printf("Number of WR edges: %d\n", m_g.allEdges().size());
		return m_g;
	}

	private boolean EncodeAndSolve() {
		Profiler prof = Profiler.getInstance();

		prof.startTick("ONESHOT_GEN_CONS");
		Set<Constraint> cons = GenConstraints(m_g);
		System.err.printf("Number of constraints: %d\n\n", cons.size());
		prof.endTick("ONESHOT_GEN_CONS");

		prof.startTick("ONESHOT_PRUNE");
		// NOTE: for TPCC, cons.size()==0, we skip this
		prof.endTick("ONESHOT_PRUNE");

		prof.startTick("ONESHOT_SOLVE");
		boolean pass = solveConstraints(m_g, cons, null);
		prof.endTick("ONESHOT_SOLVE");
		return pass;
	}

	public boolean continueslyAudit() {
		assert false;
		return false;
	}


	@Override
	public boolean audit() {
		VeriConstants.BATCH_TX_VERI_SIZE = Integer.MAX_VALUE;
		Profiler prof = Profiler.getInstance();

		// (1)
		prof.startTick("ONESHOT_CONS");
		CreateKnownGraph();
		prof.endTick("ONESHOT_CONS");

		// (2)
		boolean pass = EncodeAndSolve();

		long cons = prof.getTime("ONESHOT_CONS") + prof.getTime("ONESHOT_GEN_CONS");
		long prune = prof.getTime("ONESHOT_PRUNE");
		long solve = prof.getTime("ONESHOT_SOLVE");

		ChengLogger.println("  construct: " + cons + "ms");
		ChengLogger.println("  prune: " + prune + "ms");
		ChengLogger.println("  solve: " + solve + "ms");
		return pass;
	}


	@Override
  public int[] count() {
	  return null;
  }

}
