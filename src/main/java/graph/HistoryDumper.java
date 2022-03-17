package graph;

public interface HistoryDumper<KeyType, ValueType> {
	void dumpHistory(History<KeyType, ValueType> history);
}
