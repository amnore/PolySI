import graph.History;
import graph.HistoryLoader;
import org.junit.jupiter.api.Test;
import verifier.SIVerifier;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestVerifier {
	@Test
	void testReadCommitted() {
		var h = ((HistoryLoader<String, Integer>) () -> {
			var history = new History<String, Integer>();
			var s0 = history.addSession(0);
			var s1 = history.addSession(1);
			var t0 = history.addTransaction(s0, 0);
			var t1 = history.addTransaction(s0, 1);
			var t2 = history.addTransaction(s1, 2);

			history.addEvent(t0, true, "x", 1);
			history.addEvent(t1, true, "x", 2);
			history.addEvent(t1, true, "y", 2);
			history.addEvent(t2, false, "y", 2);
			history.addEvent(t2, false, "x", 1);
			return history;
		});

		var s = new SIVerifier<>(h);
		assertFalse(s.audit());
	}
}
