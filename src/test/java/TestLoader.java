import history.History;
import history.HistoryLoader;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;

@AllArgsConstructor
public class TestLoader implements HistoryLoader<String, Integer> {
	final Set<Long> sessions;
	final Map<Long, List<Long>> transactions;
	final Map<Long, List<Triple<History.EventType, String, Integer>>> events;

	@Override
	public History<String, Integer> loadHistory() {
		return new History<String, Integer>(sessions, transactions, events);
	}
}
