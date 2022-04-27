import history.History;
import history.HistoryLoader;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;
import verifier.SIVerifier;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static history.Event.EventType.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestVerifier {
	@Test
	void readCommitted() {
		var h = (new TestLoader(
			Set.of(0, 1),
			Map.of(0, List.of(0, 1),
				   1, List.of(2)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1)),
				   1, List.of(Triple.of(WRITE, "x", 2),
							  Triple.of(WRITE, "y", 2)),
				   2, List.of(Triple.of(READ, "y", 2),
							  Triple.of(READ, "x", 1)))
		));

		var s = new SIVerifier<>(h);
		assertFalse(s.audit());
	}

	@Test
	void repeatableRead() {
		var h = (new TestLoader(
			Set.of(0, 1),
			Map.of(0, List.of(0, 1),
				1, List.of(2)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1)),
				1, List.of(Triple.of(WRITE, "x", 2)),
				2, List.of(Triple.of(READ, "x", 1),
					Triple.of(READ, "x", 2)))));

		assertFalse(new SIVerifier<>(h).audit());
	}

	@Test
	void readMyWrites() {
		var h = (new TestLoader(
			Set.of(0, 1),
			Map.of(0, List.of(0),
				1, List.of(1, 2)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1),
					Triple.of(WRITE, "y", 1)),
				1, List.of(Triple.of(READ, "x", 1),
					Triple.of(WRITE, "y", 2)),
				2, List.of(Triple.of(READ, "x", 1),
					Triple.of(READ, "y", 1)))));

		assertFalse(new SIVerifier<>(h).audit());
	}

	@Test
	void repeatableRead2() {
		var h = (new TestLoader(
			Set.of(0, 1, 2),
			Map.of(0, List.of(0),
				1, List.of(1),
				2, List.of(2)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1),
					Triple.of(WRITE, "y", 1)),
				1, List.of(Triple.of(WRITE, "x", 2),
					Triple.of(WRITE, "y", 2)),
				2, List.of(Triple.of(READ, "x", 1),
					Triple.of(READ, "y", 2)))));

		assertFalse(new SIVerifier<>(h).audit());
	}

	@Test
	void causal() {
		var h = (new TestLoader(
			Set.of(0, 1, 2, 3),
			Map.of(0, List.of(0),
				1, List.of(1),
				2, List.of(2),
				3, List.of(3)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1)),
				1, List.of(Triple.of(READ, "x", 2),
					Triple.of(WRITE, "y", 1)),
				2, List.of(Triple.of(READ, "x", 1),
					Triple.of(WRITE, "x", 2)),
				3, List.of(Triple.of(READ, "x", 1),
					Triple.of(READ, "y", 1)))));

		assertFalse(new SIVerifier<>(h).audit());
	}

	@Test
	void prefix() {
		var h = (new TestLoader(
			Set.of(0, 1, 2, 3, 4),
			Map.of(0, List.of(0),
				1, List.of(1),
				2, List.of(2),
				3, List.of(3),
				4, List.of(4)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1),
					Triple.of(WRITE, "y", 1)),
				1, List.of(Triple.of(READ, "x", 1),
					Triple.of(WRITE, "x", 2)),
				2, List.of(Triple.of(READ, "x", 2),
					Triple.of(READ, "y", 1)),
				3, List.of(Triple.of(READ, "y", 1),
					Triple.of(WRITE, "y", 2)),
				4, List.of(Triple.of(READ, "y", 2),
					Triple.of(READ, "x", 1)))));

		assertFalse(new SIVerifier<>(h).audit());
	}

	@Test
	void conflict() {
		var h = (new TestLoader(
			Set.of(0, 1, 2),
			Map.of(0, List.of(0),
				1, List.of(1),
				2, List.of(2)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1)),
				1, List.of(Triple.of(READ, "x", 1),
					Triple.of(WRITE, "x", 2)),
				2, List.of(Triple.of(READ, "x", 1),
					Triple.of(WRITE, "x", 3)))));

		assertFalse(new SIVerifier<>(h).audit());
	}

	@Test
	void serializability() {
		var h = (new TestLoader(
			Set.of(0, 1, 2),
			Map.of(0, List.of(0),
				1, List.of(1),
				2, List.of(2)),
			Map.of(0, List.of(Triple.of(WRITE, "x", 1),
					Triple.of(WRITE, "y", 1)),
				1, List.of(Triple.of(READ, "x", 1),
					Triple.of(READ, "y", 1),
					Triple.of(WRITE, "x", 2)),
				2, List.of(Triple.of(READ, "x", 1),
					Triple.of(READ, "y", 1),
					Triple.of(WRITE, "y", 2)))));

		assertTrue(new SIVerifier<>(h).audit());
	}
}

