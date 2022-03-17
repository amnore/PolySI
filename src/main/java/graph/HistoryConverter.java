package graph;

public interface HistoryConverter<KeyType, ValueType> {
	History<Long, Long> toLongLongHistory(History<KeyType, ValueType> history);
	History<KeyType, ValueType> fromLongLongHistory(History<Long, Long> history);
}
