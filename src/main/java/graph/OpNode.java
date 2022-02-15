package graph;

import verifier.AbstractVerifier;

public class OpNode {

	// id of this op
	public final long id;

	// key and value read/written
	public final long key_hash;
	public final long val_hash;

	public final boolean isRead;

	// transaction this op belongs to
	public final long txnid;

	// for write, same as id
	// for read, the op that is read
	public final long wid;

	// for write, 0
	// for read, the txn that is read
	public final long read_from_txnid;

	// position in a txn, starting from 0
	public final int pos;

	public OpNode(boolean isRead, long txnid, String key, long val_hash, long wid, long ptid, int pos) {
		throw new AssertionError();
	}

	public OpNode(boolean isRead, long txnid, long key_hash, long val_hash, long wid, long ptid, int pos) {
		id = isRead ?
			((txnid << Integer.BYTES * 8) + pos) :
			wid; // FIXME: should guarantee the uniqueness! Assume read/write to one key in one txn
		assert pos != 0;

		this.isRead = isRead;
		this.txnid = txnid;
		this.key_hash = key_hash;
		this.val_hash = val_hash;
		this.wid = wid;
		this.read_from_txnid = ptid;
		this.pos = pos;
	}

	@Override
	public String toString() {
		return isRead
			? String.format("Read[id=%x][txnid=%x][wid=%x][w_txnid=%x][key=%x][val=%x]", id, txnid, wid, read_from_txnid, key_hash, val_hash)
			: String.format("Write[id=%x][txnid=%x][key=%x][value=%x]", id, txnid, key_hash, val_hash);
	}
}
