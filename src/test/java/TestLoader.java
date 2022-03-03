import graph.History;
import graph.HistoryLoader;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@AllArgsConstructor
public class TestLoader implements HistoryLoader<String, Integer> {
	final Set<Integer> sessions;
	final Map<Integer, List<Integer>> transactions;
	final Map<Integer, List<Triple<History.EventType, String, Integer>>> events;

	@Override
	public History<String, Integer> loadHistory() {
		var h = new History<String, Integer>();

		var sessionMap = sessions.stream()
			.map(id -> Pair.of(id, h.addSession(id)))
			.collect(Collectors.toMap(Pair::getKey, Pair::getValue));

		var txnMap = transactions.entrySet().stream()
			.flatMap(e -> e.getValue().stream().map(id -> {
				var s = sessionMap.get(e.getKey());
				return Pair.of(id, h.addTransaction(s, id));
			})).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

		events.forEach((id, list) -> list.forEach(e -> h.addEvent(
			txnMap.get(id), e.getLeft(), e.getMiddle(), e.getRight()
		)));

		return h;
	}
}
