package graph;

import java.util.ArrayList;

import util.VeriConstants;

public class TxnNode implements Cloneable {

	public long getNextClientTxn() {
		return next_client_txn;
	}

	public long getPreviousClientTxn() {
		return prev_client_txn;
	}

	public enum TxnType {
		ONGOING, COMMIT, ABORT, STALE,
	};

	// id of this txn
	public final long id;

	// txn state
	private TxnType type = TxnType.ONGOING;

	// ops in txn
	ArrayList<OpNode> ops = new ArrayList<OpNode>();

	// start and commit time
	public final long begin_timestamp;
	private long commit_timestamp = VeriConstants.TXN_NULL_TS;

	// prev and next txn of this client
	private long next_client_txn = VeriConstants.NULL_TXN_ID;
	private long prev_client_txn = VeriConstants.NULL_TXN_ID;

	public final int client_id;
	public final boolean frozen = false;  // used also as monitoring


	public TxnNode(long id, long beginTimestamp, int clientId) {
		this.id = id;
		begin_timestamp = beginTimestamp;
		client_id = clientId;
	}

	public TxnNode(long id) {
		this.id = id;
		begin_timestamp = VeriConstants.TXN_NULL_TS;
		client_id = VeriConstants.TXN_NULL_CLIENT_ID;
	}

	public long getTxnid() {
		return id();
	}

	public long id() {
		return id;
	}

	public int size() {
		return ops.size();
	}

	public OpNode get(int i) {
		return ops.get(i);
	}

	public void commit(long ts, TxnNode prevTxn) {
		type = TxnType.COMMIT;
		commit_timestamp = ts;

		if (prevTxn != null) {
			prevTxn.next_client_txn = id;
			prev_client_txn = prevTxn.id;
		}
	}

	public TxnType type() {
		return type;
	}

	public long commitTimestamp() {
		return commit_timestamp;
	}

	public void appendOp(OpNode op) {
		ops.add(op);
	}

	public ArrayList<OpNode> getOps() {
		return ops;
	}

	public String toString() {
		return "Txn[" + Long.toHexString(id()) +
			"][FZ:" + frozen +
			"][C:" + client_id + "-" + commit_timestamp +
			"][status:" + type +
			"][prev:" + Long.toHexString(getPreviousClientTxn()) +
			"][next:" + Long.toHexString(getNextClientTxn()) + "]";
	}

	public String toString3() {
		return "Txn[" + Long.toHexString(id()) + "][FZ:" + frozen + "][C:" + client_id + "-" + commit_timestamp + "]";
	}

	public String toString2() {
		StringBuilder sb = new StringBuilder();
		sb.append("Txn[" + Long.toHexString(id()) +
			"][FZ:" + frozen +
			"][C:" + client_id + "-" + commit_timestamp +
			"][status:" + type + "]{\n");
		for (OpNode op : ops) {
			sb.append("    " + op + "\n");
		}
		sb.append("}\n");
		return sb.toString();
	}

	@Override
	public TxnNode clone() {
		try {
			TxnNode clone = (TxnNode) super.clone();
			clone.ops = (ArrayList<OpNode>) ops.clone();
			return clone;
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}

}
