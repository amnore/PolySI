package graph;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import graph.History.Event;
import graph.History.EventType;
import graph.History.Session;
import graph.History.Transaction;
import graph.History.TransactionStatus;
import lombok.SneakyThrows;
import util.UnimplementedError;
import util.VeriConstants;

import static graph.History.EventType.*;

public class CobraHistoryLoader implements HistoryParser<Long, CobraHistoryLoader.CobraValue> {
	private final File logDir;

	public static long INIT_WRITE_ID = 0xbebeebeeL;
	public static long INIT_TXN_ID = 0xbebeebeeL;
	public static long NULL_TXN_ID = 0xdeadbeefL;
	public static long GC_WID_TRUE = 0x23332333L;
	public static long GC_WID_FALSE = 0x66666666L;

	public CobraHistoryLoader(Path path) {
		logDir = path.toFile();

		if (!logDir.isDirectory()) {
			throw new Error("path is not a directory");
		}
	}

	@Override
	public History<Long, CobraValue> loadHistory() {
		var files = findLogWithSuffix(".log");
		return loadLogs(files);
	}

	private ArrayList<File> findLogWithSuffix(String suffix) {
		ArrayList<File> logs = new ArrayList<File>();
		for (File f : logDir.listFiles()) {
			if (f.isFile() && f.getName().endsWith(suffix)) {
				logs.add(f);
			}
		}
		return logs;
	}

	@SneakyThrows
	private History<Long, CobraValue> loadLogs(ArrayList<File> opfiles) {
		var history = new History<Long, CobraValue>();
		var initWrites = new HashMap<Long, CobraValue>();
		var sessionId = 0;

		for (File f : opfiles) {
			try (var in = new DataInputStream(new FileInputStream(f))) {
				var session = history.addSession(sessionId++);
				extractLog(in, history, initWrites, session);
			}
		}

		var initTxn = history.addTransaction(history.addSession(INIT_TXN_ID), INIT_TXN_ID);
		for (var p : initWrites.entrySet()) {
			history.addEvent(initTxn, WRITE, p.getKey(), p.getValue());
		}

		// check all transactions except init are committed
		for (var txn : history.getTransactions()) {
			if (txn != initTxn && txn.getStatus() != TransactionStatus.COMMIT) {
				throw new InvalidHistoryError();
			}
		}

		return history;
	}

	/*
	 * Extract the graph from a stream (a byte stream from cloud or a file stream
	 * from log) with client timing edges.
	 *
	 * (startTx, txnId [, timestamp]) : 9B [+8B] <br>
	 *
	 * (commitTx, txnId [, timestamp]) : 9B [+8B] <br>
	 *
	 * (write, writeId, key_hash, val): 25B <br>
	 *
	 * (read, write_TxnId, writeId, key_hash, value) : 33B <br>
	 */
	@SneakyThrows
	public void extractLog(DataInputStream in, History<Long, CobraValue> history, Map<Long, CobraValue> initWrites,
			Session<Long, CobraValue> session) {
		Transaction<Long, CobraValue> current = null;
		while (true) {
			// break if end (for file)
			char op;
			try {
				op = (char) in.readByte();
			} catch (EOFException e) {
				break;
			}

			switch (op) {
			case 'S': {
				// TxnStart
				assert current == null;
				var id = in.readLong();

				// NOTE: because of inconsistency of the logs, the node might be created already
				// There are two possibilities:
				// 1. in graph and ongoing: continue (previous txn reads this txn)
				// 2. never seen: new TxnNode & continue
				if ((current = history.getTransaction(id)) == null) {
					current = history.addTransaction(session, id);
				} else if (current.getStatus() != TransactionStatus.ONGOING || current.getEvents().size() != 0) {
					throw new InvalidHistoryError();
				}
				break;
			}
			case 'C': {
				// TxnCommit
				var id = in.readLong();
				assert current != null && current.getId() == id;
				current.setStatus(TransactionStatus.COMMIT);
				break;
			}
			case 'W': {
				// (write, writeId, key, val): ?B <br>
				assert current != null;
				var writeId = in.readLong();
				var key = in.readLong();
				var value = in.readLong();

				// use writeId as value because cobra guarantees its uniqueness
				history.addEvent(current, WRITE, key, new CobraValue(writeId, current.getId(), value));
				break;
			}
			case 'R': {
				// (read, write_TxnId, writeId, key, value) : ?B <br>
				assert current != null;
				var writeTxnId = in.readLong();
				var writeId = in.readLong();
				var key = in.readLong();
				var value = in.readLong();

				// NOTE: if the prev_txnid == INIT_TXNID, then we update it to keyhash as wid
				// FIXME: separate NULL and INIT?
				if (writeTxnId == INIT_TXN_ID || writeTxnId == NULL_TXN_ID) {
					if (writeId == INIT_WRITE_ID || writeId == NULL_TXN_ID) {
						// NOTE: val_hash is 0 for NULL; but an arbitrary value for INIT
						writeId = key;
						writeTxnId = INIT_TXN_ID;
						// if there is no such write op in init txn, add one
						initWrites.computeIfAbsent(key, k -> new CobraValue(key, INIT_TXN_ID, value));
					} else if ((writeId != GC_WID_FALSE && writeId != GC_WID_TRUE) || writeTxnId != INIT_TXN_ID) {
						// otherwise, it should be the GC read op
						throw new InvalidHistoryError();
					}
				}

				history.addEvent(current, READ, key, new CobraValue(writeId, writeTxnId, value));
				break;
			}
			default:
				throw new InvalidHistoryError();
			}
		}
	}

	@Override
	@SneakyThrows
	public void dumpHistory(History<Long, CobraValue> history) {
		if (!logDir.isDirectory()) {
			throw new Error(String.format("%s is not a directory", logDir));
		}
		Arrays.stream(logDir.listFiles()).forEach(f -> f.delete());

		for (var session : history.getSessions()) {
			if (session.getId() == INIT_TXN_ID) {
				continue;
			}

			try (var out = new DataOutputStream(new FileOutputStream(
					logDir.toPath().resolve(String.format("T%d.log", session.getId())).toFile()))) {
				for (var transaction : session.getTransactions()) {
					out.writeByte('S');
					out.writeLong(transaction.getId());

					for (var event : transaction.getEvents()) {
						switch (event.getType()) {
						case WRITE: {
							out.writeByte('W');
							var value = event.getValue();
							out.writeLong(value.getWriteId());
							out.writeLong(event.getKey());
							out.writeLong(value.getValue());
							break;
						}
						case READ: {
							out.writeByte('R');
							var value = event.getValue();
							out.writeLong(value.getTransactionId());
							out.writeLong(value.getWriteId());
							out.writeLong(event.getKey());
							out.writeLong(value.getValue());
							break;
						}
						}
					}

					out.writeByte('C');
					out.writeLong(transaction.getId());
				}
			}
		}
	}

	@Override
	public History<Long, Long> toLongLongHistory(History<Long, CobraValue> history) {
		var newHistory = new History<Long, Long>();
		for (var session : history.getSessions()) {
			var newSession = newHistory.addSession(session.getId());
			for (var txn : session.getTransactions()) {
				var newTxn = newHistory.addTransaction(newSession, txn.getId());
				for (var ev : txn.getEvents()) {
					newHistory.addEvent(newTxn, ev.getType(), ev.getKey(), ev.getValue().writeId);
				}
			}
		}

		return newHistory;
	}

	@Override
	public History<Long, CobraValue> fromLongLongHistory(History<Long, Long> history) {
		var newHistory = new History<Long, CobraValue>();
		var writes = new HashMap<Pair<Long, Long>, Pair<Long, Long>>();

		history.getEvents().stream().filter(ev -> ev.getType() == WRITE).forEach(new Consumer<Event<Long, Long>>() {
			private long writeId = 0x10000000L;

			@Override
			public void accept(Event<Long, Long> ev) {
				writes.put(Pair.of(ev.getKey(), ev.getValue()), Pair.of(writeId++, ev.getTransaction().getId()));
			}
		});

		for (var session : history.getSessions()) {
			var newSession = newHistory.addSession(session.getId());
			for (var txn : session.getTransactions()) {
				var newTxn = newHistory.addTransaction(newSession, txn.getId());
				for (var ev : txn.getEvents()) {
					var write = writes.get(Pair.of(ev.getKey(), ev.getValue()));
					newHistory.addEvent(newTxn, ev.getType(), ev.getKey(),
							new CobraValue(write.getLeft(), write.getRight(), ev.getValue()));
				}
			}
		}

		return newHistory;
	}

	@Data
	public static class CobraValue {
		private final long writeId;
		private final long transactionId;
		private final long value;
	}
}
