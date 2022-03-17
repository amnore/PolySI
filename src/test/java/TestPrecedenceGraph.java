import graph.PrecedenceGraph;
import history.History;
import history.HistoryLoader;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static history.History.*;
import static history.History.EventType.*;
import static org.junit.jupiter.api.Assertions.*;

public class TestPrecedenceGraph {
	@Test
	void test1() {
		var h = (new TestLoader(
			Set.of(1, 2),
			Map.of(1, List.of(0, 2),
				2, List.of(1)),
			Map.of(0, List.of(Triple.of(WRITE, "0", 0)),
				1, List.of(Triple.of(READ, "0", 0)),
				2, List.of(Triple.of(READ, "0", 0),
					Triple.of(WRITE, "0", 1)))
		)).loadHistory();

		var g = new PrecedenceGraph<>(h);
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
