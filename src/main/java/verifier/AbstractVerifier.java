package verifier;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import graph.EdgeType;
import graph.OpNode;
import graph.PrecedenceGraph;
import graph.TxnNode;
import graph.TxnNode.TxnType;
import net.openhft.hashing.LongHashFunction;
import util.ChengLogger;
import util.Profiler;
import util.VeriConstants;
import util.VeriConstants.LoggerType;


/*
 * There are four type of edges:
 * (1) client-order edge: ExtractClientLogFromStream()
 * (2) RW edge: ExtractDBReportFromStream()
 * (3) WW edge: ExtractDBReportFromStream()
 * (4) WR edge: AddReadFromEdge()
 */

public abstract class AbstractVerifier {

	// some statistics
	//   about workload
	public int num_rmw_ww = 0;
	public int num_ops = 0;
	public int num_reads = 0;
	public int num_writes = 0;
	public int num_txns = 0;
	public int max_concurrent_txns = -1;
	//   about edges
	public int num_wr = 0;
	public int num_rw = 0;  // this is not accurate; see AddWREdges's duplication
	public int num_co = 0;
	public int num_to = 0;
	public int num_anti_ww = 0;
	public int num_infer_ww = 0;
	public int num_unknown_rw = 0;
	// constraints
	public int num_merged_constraints = 0;
	// whether run out of tim (OOT)
	public boolean OOT = false;

	// verifier information
	protected PrecedenceGraph m_g = null;
	// client related:
	public ArrayList<String> client_list = null;
	public Map<String, Integer> client2id = null;
	// last transaction of each client
	public ArrayList<Long> last_txn = null;
	public Map<Integer,Integer> client_txn_counter = null;
	// we need to remember deleted transaction ids
	// in order to re-insert the deleted txns
	protected Set<Long> deleted_txnids;
	// some meta-data
	Map<Long, Long> wid2txnid = null; // will be completely renewed in AddRWEdges() and updated in _do_gc_query()

	// track all the keys in the init tx (this is a performance issue)
	private Set<Long> init_txn_keys = null;
	// tack all the ongoing txns
	protected Set<TxnNode> potential_ongoing_txns = null;

	// if remote logging from socket
	protected boolean remote_log = false;

	// epoch and round
	public Set<TxnNode> new_txns_this_turn;
	public int epoch_agree = 0;
	public int round = 0;
	public Map<Long, Set<Long>> frontier = null;


	public AbstractVerifier() {
		m_g = new PrecedenceGraph();

		// NOTE: this is an assumption, that there is an initial transaction
		// which indicates the initial state of the database
		/*   -- (1) add it at very beginning
         -- (2) do not delete this
         -- (3) during graph construction, give each key we met with INIT_WID a OpNode
                and for each of the operations, we also set INIT_WID to keyhash
         -- (4) assert that we should not see any INIT_WID in the following process, since we rm them
         -- (5) INIT txn happens before all txns
		 */
		TxnNode init = new TxnNode(VeriConstants.INIT_TXN_ID);
		// init txn should be always ONGOING
		//init.commit(0);
		m_g.addTxnNode(init);

		client_list = new ArrayList<String>();
		client2id = new HashMap<String, Integer>();
		last_txn = new ArrayList<Long>();
		client_txn_counter = new HashMap<Integer,Integer>();
		deleted_txnids = new HashSet<Long>();
		init_txn_keys = new HashSet<Long>();
		potential_ongoing_txns = new HashSet<TxnNode>();
		new_txns_this_turn = new HashSet<TxnNode>();

		// initialize INIT_TXN with GC ops
		//GarbageCollector.addGCops2Init(init, init_txn_keys);
	}

	public PrecedenceGraph getGraph() {return m_g;}

	abstract public boolean audit();
	abstract public int[] count();
	abstract public boolean continueslyAudit();

	// ======= client names ========

	public int getClientId(String name) {
		if (client2id.containsKey(name)) {
			assert name.equals(client_list.get(client2id.get(name)));
			return client2id.get(name);
		}
		// we've never seen this name
		for (String seen_name : client_list) assert !seen_name.equals(name);
		return VeriConstants.TXN_NULL_CLIENT_ID;
	}

	public long ExtractClientLogFromStream(DataInputStream in, PrecedenceGraph g, String client_name) throws IOException {
		int cid = getClientId(client_name);
		// if we first see this client
		if (cid == VeriConstants.TXN_NULL_CLIENT_ID) {
			cid = client_list.size();
			client_list.add(client_name);
			last_txn.add(VeriConstants.NULL_TXN_ID);
			client_txn_counter.put(cid, 0);
			client2id.put(client_name, cid);
		}

		//System.out.println("Client name = " + client_name);
		long last_txnid = ExtractClientLogFromStream(in, g, last_txn.get(cid), client_txn_counter, cid);
		last_txn.set(cid, last_txnid);
		return last_txnid;
	}


	public static long hashKey(String key) {
		return LongHashFunction.xx().hashChars(key);
	}

	/*
	 * Extract the graph from a stream
	 *   (a byte stream from cloud
	 *   or a file stream from log)
	 * with client timing edges.
	 */
	public long ExtractClientLogFromStream(DataInputStream in, PrecedenceGraph g, long recent_txnid, Map<Integer, Integer> client_txn_counter, int client_id) throws IOException {
		/**
		 * (startTx, txnId [, timestamp]) : 9B [+8B] <br>
		 * (commitTx, txnId [, timestamp]) : 9B [+8B] <br>
		 * (write, writeId, key_hash, val): 25B <br>
		 * (read, write_TxnId, writeId, key_hash, value) : 33B <br>
		 */

		int parsed_txn_counter = 0;
		boolean reach_parse_bound = false;
		int txn_counter = client_txn_counter.get(client_id);

		TxnNode cur_txn = null;
		TxnNode prev_txn = g.getNode(recent_txnid); // will be null if there is no such txn
		assert prev_txn == null /*init*/ || prev_txn.client_id == client_id;
		char op_type;
		int op_counter = 0;
		long wid = 0, key_hash = 0, val_hash = 0, prev_txnid = 0, txnid = 0;

		while(true) {
			// break if not enough data (for socket; when beginning)
			if (remote_log && op_counter == 0 && in.available() < VeriConstants.MAX_BYTES_IN_TXN) {
				break;
			}
			// break if end (for file)
			try {
				op_type = (char) in.readByte();
			} catch (EOFException e) {
				break;
			}

			switch(op_type) {
			case 'S':
				txnid = in.readLong();
				// TxnStart
				assert cur_txn == null;
				// NOTE: because of inconsistency of the logs, the node might be created already
				cur_txn = g.getNode(txnid);
				// There are two possibilities:
				//  1. in graph and ongoing:    continue (previous txn reads this txn)
				//  2. never seen:              new TxnNode & continue
				if (cur_txn != null) { // case 1
					assert cur_txn.type() == TxnType.ONGOING;
					assert cur_txn.getOps().size() == 0;
				} else {  // case 2
					var beginTs = Math.abs(txnid << 3);
					cur_txn = new TxnNode(txnid, beginTs, client_id);
					g.addTxnNode(cur_txn);
				}
				//System.out.print("S[" + Long.toHexString(txnid) + "]->");
				op_counter = 0;
				break;
			case 'C':
				// TxnCommit
				assert cur_txn != null;
				txnid = in.readLong();
				assert txnid == cur_txn.getTxnid();
				cur_txn.commit(txn_counter++, prev_txn);
				// FIXME: which is better? (1) add edges from init to all txns; or (2) add edges
				// to only those without predecessor
				g.addEdge(g.getNode(VeriConstants.INIT_TXN_ID), cur_txn, EdgeType.INIT);
				// add client-order edge
				if (prev_txn != null) {
					g.addEdge(prev_txn, cur_txn, EdgeType.CO);
					this.num_co++;
				}
				// this is a new txn the parser sees
				new_txns_this_turn.add(cur_txn);
				//System.out.println("C[" + Long.toHexString(txnid) + "]");
				prev_txn = cur_txn;
				cur_txn = null;
				this.num_txns++;
				// stop parsing when: (1) reach enough from this client; (2) not enough data in socket
				parsed_txn_counter++;
				if (parsed_txn_counter >= VeriConstants.BATCH_TX_VERI_SIZE) { // (1)
					reach_parse_bound = true;
					break;
				}
				if (remote_log && in.available() < VeriConstants.MAX_BYTES_IN_TXN) { // (2)
					reach_parse_bound = true;
					break;
				}
				break;
			case 'A':
				// if txn abort, it will not appear in the log.
				assert false;
				break;
			case 'W': {
				// (write, writeId, key_len, key, val): ?B <br>
				wid =in.readLong();
				key_hash = in.readLong();
				val_hash = in.readLong();
				OpNode w = new OpNode(false, txnid, key_hash, val_hash, wid, 0/* prev_txnid */, op_counter);
				cur_txn.appendOp(w);
				//System.out.print("W[" + Long.toHexString(txnid) + "]<" + Long.toHexString(key_hash) + "> ");
				this.num_writes++;
				break; }
			case 'R': {
				// (read, write_TxnId, writeId, key_len, key, value) : ?B <br>
				prev_txnid = in.readLong();
				wid = in.readLong();
				key_hash = in.readLong();
				val_hash = in.readLong();
				// NOTE: if the prev_txnid == INIT_TXNID, then we update it to keyhash as wid
				// FIXME: separate NULL and INIT?
				if (prev_txnid == VeriConstants.INIT_TXN_ID || prev_txnid == VeriConstants.NULL_TXN_ID) {
					if (wid == VeriConstants.INIT_WRITE_ID || wid == VeriConstants.NULL_TXN_ID) {
						// NOTE: val_hash is 0 for NULL; but an arbitrary value for INIT
						wid = key_hash; // change the INIT_WRITE_ID => key_hash
						prev_txnid = VeriConstants.INIT_TXN_ID;
						// if there is no such write op in init txn, add one
						addWriteIfNotExist(g, key_hash, val_hash);
					} else { // otherwise, it should be the GC read op
						assert wid == VeriConstants.GC_WID_TRUE || wid == VeriConstants.GC_WID_FALSE;
						assert prev_txnid == VeriConstants.INIT_TXN_ID;
					}
				}
				cur_txn.appendOp(new OpNode(true, txnid, key_hash, val_hash, wid, prev_txnid, op_counter));
				//System.out.print("R[" + Long.toHexString(txnid) + "]<" + Long.toHexString(key_hash) + "> ");
				this.num_reads++;
				break; }
			default:
				ChengLogger.println(LoggerType.ERROR, "UNKOWN op type [" + op_type + "]");
				assert false;
			}  // end of switch
			this.num_ops++;
			op_counter++;

			if(reach_parse_bound) {
				break;
			}
		} // end of while

		//System.out.println("====finish CID[" + client_id + "] #txn[" + txn_counter + "]====");

		// update the txn counter of current client
		client_txn_counter.put(client_id, txn_counter);
		assert cur_txn == null; // assert this is then end of one txn or there is no txn at all
		// if we didn't find any transaction, just return what we got
		return (prev_txn == null) ? recent_txnid : prev_txn.getTxnid();
	}

	private void addWriteIfNotExist(PrecedenceGraph g, long key_hash, long val_hash) {
		boolean has_write = init_txn_keys.contains(key_hash);
		if (!has_write) {
			TxnNode init_t = g.getNode(VeriConstants.INIT_TXN_ID);
			OpNode init_w = new OpNode(false, VeriConstants.INIT_TXN_ID, key_hash, val_hash,
					key_hash /* wid */,
					0 /* prev_txnid */,
					init_t.getOps().size() + 1 /* pos */);
			init_t.appendOp(init_w);
			init_txn_keys.add(key_hash);
		}
	}

	public static void AddWREdges(PrecedenceGraph g, Set<TxnNode> new_txns_this_turn, AbstractVerifier veri) {
		for (TxnNode txn : new_txns_this_turn) {
			long cur_txnid = txn.getTxnid();
			for (OpNode op : txn.getOps()) {
				if (op.isRead) {
					long dependent_txnid = op.read_from_txnid;
					if (dependent_txnid == cur_txnid) { // does not allow read from itself
						for (OpNode tmp : g.getNode(cur_txnid).getOps()) {
							ChengLogger.println(LoggerType.ERROR, "    " + tmp.toString());
						}
						assert false;
					}

					if (veri!=null && veri.deleted_txnids.contains(dependent_txnid)) {
						assert false;
					}

					// NOTE: it is possible that the log is inconsistent that some client-log is "faster" than others,
					// so we may see that one edge is point to somewhere unknown, just skip (it will be seen eventually)
					if (g.getNode(dependent_txnid) == null) {
						assert veri != null; // this cannot happen when constructing complete graph
						// add an empty node, but the edges will be created to this empty node
						g.addTxnNode(new TxnNode(dependent_txnid));
						// add this ONGOING txn into ongoing set
						veri.potential_ongoing_txns.add(g.getNode(dependent_txnid));
					}
					g.addEdge(dependent_txnid, cur_txnid, EdgeType.WR);
					if (veri != null) {
						veri.num_wr++;
					}
					long prev_wid = op.wid;

					if (!g.m_readFromMapping.containsKey(prev_wid)) {
						g.m_readFromMapping.put(prev_wid, new HashSet<OpNode>());
					}
					g.m_readFromMapping.get(prev_wid).add(op);
				}
			}
		}
	}


	public void UpdateWid2Txnid(PrecedenceGraph g, Set<TxnNode> new_txns_this_turn) {
		if (wid2txnid == null) {
			wid2txnid = new HashMap<Long,Long>();
		}

		// construct all current known wid->txnid
		for (TxnNode txn : new_txns_this_turn) {
			assert !deleted_txnids.contains(txn.getTxnid());
			for (OpNode op : txn.getOps()) {
				if (op.isRead) {
					if (op.key_hash == op.wid) { // if read from init; add it here
						wid2txnid.put(op.wid, VeriConstants.INIT_TXN_ID);
					}
				} else { // write
					if (wid2txnid.containsKey(op.wid)) {
						ChengLogger.println(LoggerType.ERROR, "duplicated wid: " + Long.toHexString(op.wid));
						ChengLogger.println(LoggerType.ERROR, "<<<===previous txn:");
						ChengLogger.println(LoggerType.ERROR, g.getNode(wid2txnid.get(op.wid)).toString2());
						ChengLogger.println(LoggerType.ERROR, ">>>=====current txn:");
						ChengLogger.println(LoggerType.ERROR, txn.toString2());
						assert false;
					}
					wid2txnid.put(op.wid, op.txnid);
				}
			}
		}
	}

	public static void CheckValues(PrecedenceGraph pg) {
		for (TxnNode n : pg.allNodes()) {
			for (OpNode op : n.getOps()) {
				// check all reads read the same value from the corresponding writes
				if (op.isRead) { continue; }
				if (!pg.m_readFromMapping.containsKey(op.wid)) { continue; }
				for (OpNode rop : pg.m_readFromMapping.get(op.wid)) {
					assert op.val_hash == rop.val_hash;
				}
			}
		}
	}

	// === show results ===

	public String profResults() {
		StringBuilder sb = new StringBuilder();
		Profiler prof = Profiler.getInstance();
		sb.append("=======Performance Results========\n");
		sb.append("1. Init\n");
		sb.append("  log prasing:" + prof.getTime(VeriConstants.PROF_OFFLINE_LOG_LOADING_TIME) + "ms\n");
		sb.append("    -> loading file: " + prof.getTime(VeriConstants.PROF_FILE_LOADING) + "ms\n");
		sb.append("    -> parsing log: " + prof.getTime(VeriConstants.PROF_LOG_PARSING) + "ms\n");
		sb.append("2. Bookkeeping\n");
		sb.append("  Transitive closure (GPU):" + prof.getTime(VeriConstants.PROF_GPU_MM_TIME) + "ms\n");
		sb.append("  polygraph build:" + prof.getTime(VeriConstants.PROF_POLY_GRAPH) + "ms\n");
		sb.append("    -> 1:" + prof.getTime(VeriConstants.PROF_POLY_GRAPH1) + "ms\n");
		sb.append("    -> 2:" + prof.getTime(VeriConstants.PROF_POLY_GRAPH2) + "ms\n");
		sb.append("    -> 3:" + prof.getTime(VeriConstants.PROF_POLY_GRAPH3) + "ms\n");
		sb.append("  solve constraints:" + prof.getTime(VeriConstants.PROF_SOLVE_CONSTRAINTS) + "ms\n");
		sb.append("    -> 1:" + prof.getTime(VeriConstants.PROF_SOLVE_CONSTRAINTS1) + "ms, count="
												  + prof.getCounter(VeriConstants.PROF_SOLVE_CONSTRAINTS1) + "\n");
		sb.append("    -> 2:" + prof.getTime(VeriConstants.PROF_SOLVE_CONSTRAINTS2) + "ms, count="
			                    + prof.getCounter(VeriConstants.PROF_SOLVE_CONSTRAINTS2) + "\n");
		sb.append("    -> 3:" + prof.getTime(VeriConstants.PROF_SOLVE_CONSTRAINTS3) + "ms, count="
			                    + prof.getCounter(VeriConstants.PROF_SOLVE_CONSTRAINTS3) + "\n");
		sb.append("  merge constraints:" + prof.getTime(VeriConstants.PROF_MERGE_CONSTRAINTS) + "ms\n");
		sb.append("3. PCSG\n");
		sb.append("  gen PCSGs:" + prof.getTime(VeriConstants.PROF_PCSG_TIME) + "ms\n");
		sb.append("    -> speculative:" + prof.getTime(VeriConstants.PROF_PCSG_TIME_1)+"ms\n");
		sb.append("    -> scc:" + prof.getTime(VeriConstants.PROF_PCSG_TIME_2)+"ms\n");
		sb.append("    -> subgraph:" + prof.getTime(VeriConstants.PROF_PCSG_TIME_3)+"ms\n");
		sb.append("4. Search\n");
		if (this.OOT) {
			sb.append("  search time: OOT\n");
		} else {
			sb.append("  search time:" + prof.getTime(VeriConstants.PROF_SEARCH) + "ms\n");
			sb.append("    -> 1:" + prof.getTime(VeriConstants.PROF_MONOSAT_1) + "ms\n");
			sb.append("    -> 2:" + prof.getTime(VeriConstants.PROF_MONOSAT_2) + "ms\n");
		}
		sb.append("5. Trace truncation\n");
		sb.append("  truncation time:" + prof.getTime(VeriConstants.PROF_TRUNCATION) + "ms\n");
		sb.append("    -> init versioning:" + prof.getTime(VeriConstants.PROF_TRUNCATION_1)+"ms\n");
		sb.append("    -> calculate safe deletion:" + prof.getTime(VeriConstants.PROF_TRUNCATION_2)+"ms\n");
		sb.append("    -> delete from graph:" + prof.getTime(VeriConstants.PROF_TRUNCATION_3)+"ms\n");
		sb.append("    -> transitive reduction:" + prof.getTime(VeriConstants.PROF_TRANSITIVE_REDUCTION)+"ms\n");
		return sb.toString();
	}

	public String statisticsResults() {
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);

		StringBuilder sb = new StringBuilder();
		sb.append("=======Benchmark Statistics========\n");
		sb.append("---algo parameters---\n");
		sb.append("Workload=" + VeriConstants.LOG_FD_LOG + "\n");
		sb.append("TO_ON=" + VeriConstants.TIME_ORDER_ON + "\n");
		sb.append("INFER_REL_ON=" + VeriConstants.INFER_RELATION_ON + "\n");
		sb.append("PCSG_ON=" + VeriConstants.PCSG_ON + "\n");
		sb.append("WRITE_SPACE_ON=" + VeriConstants.WRITE_SPACE_ON + "\n");
		sb.append("---workload---\n");
		sb.append("#txns=" + this.num_txns + "\n");
		sb.append("concurrent=" + this.max_concurrent_txns + "\n");
		sb.append("#clients=" + this.client_list.size() + "\n");
		sb.append("#ops=" + this.num_ops + "\n");
		sb.append("#reads=" + this.num_reads + "\n");
		sb.append("#writes=" + this.num_writes + "\n");
		sb.append("read:write=" + df.format((double)this.num_reads / this.num_writes) + "\n");
		sb.append("---edges---\n");
		sb.append("#rmw=" + this.num_rmw_ww + "\n");
		sb.append("#wr=" + this.num_wr + "\n");
		sb.append("#rw=" + this.num_rw + "\n");
		sb.append("#co=" + this.num_co + "\n");
		sb.append("#to=" + this.num_to + "\n");
		sb.append("#anti-ww=" + this.num_anti_ww + "\n");
		sb.append("#infer-ww=" + this.num_infer_ww + "\n");
		//sb.append("#unknow-read=" + this.num_unknown_rw + "\n");
		sb.append("-----graph-----\n");
		sb.append("graph's #node=" + m_g.allNodes().size() + "\n");
		sb.append("graph's #edges=" + m_g.allEdges().size() + "\n");

		return sb.toString();
	}

}
