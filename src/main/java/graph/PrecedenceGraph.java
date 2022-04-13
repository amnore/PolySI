package graph;

import static history.History.EventType.READ;
import static history.History.EventType.WRITE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraph;
import com.google.common.graph.ValueGraphBuilder;

import org.apache.commons.lang3.tuple.Pair;

import history.History;
import history.History.Transaction;

@SuppressWarnings("UnstableApiUsage")
public class PrecedenceGraph<KeyType, ValueType> {
	private final MutableValueGraph<Transaction<KeyType, ValueType>, Set<KeyType>> readFromGraph = ValueGraphBuilder.directed().build();
	private final MutableGraph<Transaction<KeyType, ValueType>> knownGraphA = GraphBuilder.directed().build();
	private final MutableGraph<Transaction<KeyType, ValueType>> knownGraphB = GraphBuilder.directed().build();

	/**
	 * Build a graph from a history
	 *
	 * The built graph contains SO and WR edges
	 */
	public PrecedenceGraph(History<KeyType, ValueType> history) {
		history.getTransactions().forEach(txn -> {
			knownGraphA.addNode(txn);
			knownGraphB.addNode(txn);
			readFromGraph.addNode(txn);
		});

		// add SO edges
		history.getSessions().forEach(session -> {
			Transaction<KeyType, ValueType> prevTxn = null;
			for (var txn : session.getTransactions()) {
				if (prevTxn != null) {
					knownGraphA.putEdge(prevTxn, txn);
				}
				prevTxn = txn;
			}
		});

		// add WR edges
		var writes = new HashMap<Pair<KeyType, ValueType>, Transaction<KeyType, ValueType>>();
		var events = history.getEvents();
		events.stream().filter(e -> e.getType() == WRITE)
				.forEach(ev -> writes.put(Pair.of(ev.getKey(), ev.getValue()), ev.getTransaction()));
		events.stream().filter(e -> e.getType() == READ).forEach(ev -> {
			var writeTxn = writes.get(Pair.of(ev.getKey(), ev.getValue()));
			var txn = ev.getTransaction();

			if (writeTxn == txn) {
				return;
			}

			if (!readFromGraph.hasEdgeConnecting(writeTxn, txn)) {
				readFromGraph.putEdgeValue(writeTxn, txn, new HashSet<>());
			}
			readFromGraph.edgeValue(writeTxn, txn).get().add(ev.getKey());
			knownGraphA.putEdge(writeTxn, txn);
		});
	}

	public ValueGraph<Transaction<KeyType, ValueType>, Set<KeyType>> getReadFrom() {
		return readFromGraph;
	}

	public Graph<Transaction<KeyType, ValueType>> getKnownGraphA() {
		return knownGraphA;
	}

	public Graph<Transaction<KeyType, ValueType>> getKnownGraphB() {
		return knownGraphB;
	}

	public void putEdgeToGraphA(Transaction<KeyType, ValueType> u, Transaction<KeyType, ValueType> v) {
		knownGraphA.putEdge(u, v);
		if (knownGraphB.hasEdgeConnecting(u, v)) {
			knownGraphB.removeEdge(u, v);
		}
	}

	public void putEdgeToGraphB(Transaction<KeyType, ValueType> u, Transaction<KeyType, ValueType> v) {
		if (!knownGraphA.hasEdgeConnecting(u, v)) {
			knownGraphB.putEdge(u, v);
		}
	}
}
