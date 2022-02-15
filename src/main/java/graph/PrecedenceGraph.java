package graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.MutableGraph;

import graph.TxnNode.TxnType;
import util.ChengLogger;
import util.VeriConstants;
import util.VeriConstants.LoggerType;


// a wrapper for graph
public class PrecedenceGraph {

	private MutableGraph<TxnNode> g;
	// mapping txnid=>node
	private HashMap<Long,TxnNode> tindex;
	// read-from dependency, wid => {OpNode, ...}
	public Map<Long, Set<OpNode>> m_readFromMapping = null;



	// ======Constructors========

	public PrecedenceGraph() {
		g = GraphBuilder.directed().allowsSelfLoops(false).build();
		tindex = new HashMap<>();
		m_readFromMapping = new HashMap<>();
	}
	// ======Graph=======

	public MutableGraph<TxnNode> getGraph() {
		return g;
	}


	// =======Nodes=========

	public Collection<TxnNode> allNodes() {
		return tindex.values();
	}

	public Set<Long> allTxnids() {
		return tindex.keySet();
	}

	public boolean containTxnid(long id) {
		return tindex.containsKey(id);
	}

	public TxnNode getNode(long id) {
		if (tindex.containsKey(id)) {
			return tindex.get(id);
		}
		return null;
	}

	public void addTxnNode(TxnNode n) {
		// because of the inconsistency of logs, we may create an empty node,
		assert !tindex.containsKey(n.getTxnid());
		g.addNode(n);
		tindex.put(n.getTxnid(), n);
	}

	public Set<TxnNode> successors(TxnNode n) {
		return g.successors(n);
	}

	public Set<TxnNode> predecessors(TxnNode n) {
		return g.predecessors(n);
	}

	// =====Edges=====

	public Set<EndpointPair<TxnNode>> allEdges() {
		return g.edges();
	}

	public void addEdge(long fr, long to, EdgeType et) {
		assert tindex.containsKey(fr) && tindex.containsKey(to);
		TxnNode src = tindex.get(fr);
		TxnNode dst = tindex.get(to);
		g.putEdge(src, dst);
	}

	public void addEdge(TxnNode fr, TxnNode to, EdgeType et) {
		assert tindex.containsKey(fr.getTxnid()) && tindex.containsKey(to.getTxnid());
		g.putEdge(fr, to);
	}

	// =====WW dependency=====

	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("PrecedenceGraph. #txn=" + allNodes().size() + " #edges=" + allEdges().size() + "\n");
		ret.append("                 #w_has_read=" + m_readFromMapping.size() + "\n");
		return ret.toString();
	}

	/*
	 * Check whether this graph is complete, which means all transactions are
	 * commited, except the initial one.
	 */
	public boolean isComplete() {
		for (var txn : allNodes()) {
			if (txn.type() != TxnType.COMMIT
					&& txn.getTxnid() != VeriConstants.INIT_TXN_ID) {
				return false;
			}
		}

		return true;
	}

	/*
	 * Check whether all reads read the same value from the corresponding write
	 */
	public boolean readWriteMatches() {
		for (var txn : allNodes()) {
			for (OpNode op : txn.getOps()) {
				if (op.isRead || !m_readFromMapping.containsKey(op.wid)) {
					continue;
				}

				for (OpNode rop : m_readFromMapping.get(op.wid)) {
					if (op.val_hash != rop.val_hash) {
						return false;
					}
				}
			}
		}

		return true;
	}
}
