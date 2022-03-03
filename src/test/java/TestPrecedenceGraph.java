import graph.History;
import graph.HistoryLoader;
import graph.PrecedenceGraph2;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class TestPrecedenceGraph {
	@Test
	void test1() {
		var h = ((HistoryLoader<Integer, Integer>) () -> {
			var h1 = new History<Integer, Integer>();
			var s1 = h1.addSession(1);
			var s2 = h1.addSession(2);

			var t0 = h1.addTransaction(s1, 0);
			var t1 = h1.addTransaction(s2, 1);
			var t2 = h1.addTransaction(s1, 2);

			h1.addEvent(t0, true, 0, 0);
			h1.addEvent(t1, false, 0, 0);
			h1.addEvent(t2, false, 0, 0);
			h1.addEvent(t2, true, 0, 1);

			return h1;
		}).loadHistory();

		var g = new PrecedenceGraph2<>(h);
		var rf = g.getReadFrom();
		var so = g.getSessionOrder();

		var t0 = h.getTransaction(0);
		var t1 = h.getTransaction(1);
		var t2 = h.getTransaction(2);
		assertEquals(Set.of(t1, t2), rf.successors(t0));
		assertEquals(Set.of(), rf.successors(t1));
		assertEquals(Set.of(), rf.successors(t2));
		assertEquals(Set.of(t2), so.successors(t0));
		assertEquals(Set.of(), so.successors(t1));
		assertEquals(Set.of(), so.successors(t2));
	}
}
