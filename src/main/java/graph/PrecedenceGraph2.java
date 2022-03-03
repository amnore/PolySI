package graph;

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

import graph.History.Transaction;

import static graph.History.EventType.READ;
import static graph.History.EventType.WRITE;

@SuppressWarnings("UnstableApiUsage")
public class PrecedenceGraph2<KeyType, ValueType> {
	private final MutableGraph<Transaction<KeyType, ValueType>> sessionOrderGraph = GraphBuilder.directed().build();
	private final MutableValueGraph<Transaction<KeyType, ValueType>, Set<KeyType>> readFromGraph = ValueGraphBuilder.directed().build();

	/**
	 * Build a graph from a history
	 *
	 * The built graph contains SO and WR edges
	 */
	public PrecedenceGraph2(History<KeyType, ValueType> history) {
		history.getTransactions().forEach(txn -> {
			sessionOrderGraph.addNode(txn);
			readFromGraph.addNode(txn);
		});

		// add SO edges
		history.getSessions().forEach(session -> {
			Transaction<KeyType, ValueType> prevTxn = null;
			for (var txn : session.getTransactions()) {
				if (prevTxn != null) {
					sessionOrderGraph.putEdge(prevTxn, txn);
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
			if (!readFromGraph.hasEdgeConnecting(writeTxn, txn)) {
				readFromGraph.putEdgeValue(writeTxn, txn, new HashSet<>());
			}
			readFromGraph.edgeValue(writeTxn, txn).get().add(ev.getKey());
		});
	}

	public Graph<Transaction<KeyType, ValueType>> getSessionOrder() {
		return sessionOrderGraph;
	}

	public ValueGraph<Transaction<KeyType, ValueType>, Set<KeyType>> getReadFrom() {
		return readFromGraph;
	}
}
