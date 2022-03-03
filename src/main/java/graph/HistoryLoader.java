package graph;

public interface HistoryLoader<KeyType, ValueType> {
	public History<KeyType, ValueType> loadHistory();
}
