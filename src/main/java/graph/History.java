package graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

import lombok.Data;
import lombok.EqualsAndHashCode;
import util.UnimplementedError;

public class History<KeyType, ValueType> {
	private final Map<Long, Session<KeyType, ValueType>> sessions = new HashMap<>();
	private final Map<Long, Transaction<KeyType, ValueType>> transactions = new HashMap<>();
	private final Set<Pair<KeyType, ValueType>> writes = new HashSet<>();

	public Collection<Session<KeyType, ValueType>> getSessions() {
		return sessions.values();
	}

	public Collection<Transaction<KeyType, ValueType>> getTransactions() {
		return transactions.values();
	}

	public Collection<Event<KeyType, ValueType>> getEvents() {
		return transactions.values().stream().flatMap(txn -> txn.events.stream()).collect(Collectors.toList());
	}

	public Session<KeyType, ValueType> getSession(long id) {
		return sessions.get(id);
	}

	public Transaction<KeyType, ValueType> getTransaction(long id) {
		return transactions.get(id);
	}

	public Session<KeyType, ValueType> addSession(long id) {
		if (sessions.containsKey(id)) {
			throw new InvalidHistoryError();
		}

		var session = new Session<KeyType, ValueType>(id);
		sessions.put(id, session);
		return session;
	}

	public Transaction<KeyType, ValueType> addTransaction(Session<KeyType, ValueType> session, long id) {
		if (!sessions.containsKey(session.id) || transactions.containsKey(id)) {
			throw new InvalidHistoryError();
		}

		var txn = new Transaction<KeyType, ValueType>(id, session);
		transactions.put(id, txn);
		session.getTransactions().add(txn);
		return txn;
	}

	public Event<KeyType, ValueType> addEvent(Transaction<KeyType, ValueType> transaction, boolean write, KeyType key, ValueType value) {
		var p = Pair.of(key, value);
		if (write) {
			if (!transactions.containsKey(transaction.id) || writes.contains(p)) {
				throw new InvalidHistoryError();
			}
			writes.add(p);
		}

		var ev = new Event<KeyType, ValueType>(transaction, write, key, value);
		transaction.getEvents().add(ev);
		return ev;
	}

	@Data
	@EqualsAndHashCode(onlyExplicitlyIncluded = true)
	public static class Session<KeyType, ValueType> {
		@EqualsAndHashCode.Include
		private final long id;

		private final List<Transaction<KeyType, ValueType>> transactions = new ArrayList<>();
	}

	@Data
	@EqualsAndHashCode(onlyExplicitlyIncluded = true)
	@ToString(onlyExplicitlyIncluded = true)
	public static class Transaction<KeyType, ValueType> {
		@EqualsAndHashCode.Include
		@ToString.Include
		private final long id;

		private final Session<KeyType, ValueType> session;
		private final List<Event<KeyType, ValueType>> events = new ArrayList<>();
		private TransactionStatus status = TransactionStatus.ONGOING;
	}

	@Data
	@EqualsAndHashCode(onlyExplicitlyIncluded = true)
	@ToString(onlyExplicitlyIncluded = true)
	public static class Event<KeyType, ValueType> {
		private final Transaction<KeyType, ValueType> transaction;

		@EqualsAndHashCode.Include
		@ToString.Include
		private final boolean write;

		@EqualsAndHashCode.Include
		@ToString.Include
		private final KeyType key;

		@EqualsAndHashCode.Include
		@ToString.Include
		private final ValueType value;
	}

	public enum TransactionStatus {
		ONGOING, COMMIT
	}
}
