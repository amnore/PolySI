package graph;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import com.google.common.io.LittleEndianDataInputStream;

import graph.History.Session;
import lombok.SneakyThrows;
import util.UnimplementedError;

import static graph.History.EventType.READ;
import static graph.History.EventType.WRITE;

@SuppressWarnings("UnstableApiUsage")
public class DBCopHistoryLoader implements HistoryLoader<Long, Long> {
	private final File logFile;
	private History<Long, Long> history;
	private long sessionId = 1;
	private long transactionId = 1;

	public DBCopHistoryLoader(Path logPath) {
		logFile = logPath.toFile();

		if (!logFile.isFile()) {
			throw new Error("file does not exist");
		}
	}

	@Override
	@SneakyThrows
	public History<Long, Long> loadHistory() {
		if (history != null) {
			return history;
		}

		history = new History<>();
		try (var in = new LittleEndianDataInputStream(new FileInputStream(logFile))) {
			return parseHistory(in);
		}
	}

	@SneakyThrows
	private History<Long, Long> parseHistory(LittleEndianDataInputStream in) {
		var id = in.readLong();
		var nodeNum = in.readLong();
		var variableNum = in.readLong();
		var transactionNum = in.readLong();
		var eventNum = in.readLong();
		var info = parseString(in);
		var start = parseString(in);
		var end = parseString(in);

		var length = in.readLong();
		for (long i = 0; i < length; i++) {
			parseSession(in);
		}

		if (history.getSessions().size() != nodeNum || history.getTransactions().size() != transactionNum
				|| history.getEvents().size() != eventNum) {
			throw new InvalidHistoryError();
		}

		return history;
	}

	@SneakyThrows
	private String parseString(LittleEndianDataInputStream in) {
		var size = in.readLong();
		assert size <= Integer.MAX_VALUE;
		return new String(in.readNBytes((int) size), StandardCharsets.UTF_8);
	}

	@SneakyThrows
	void parseSession(LittleEndianDataInputStream in) {
		var length = in.readLong();
		var session = history.addSession(sessionId++);
		for (long i = 0; i < length; i++) {
			parseTransaction(session, in);
		}
	}

	@SneakyThrows
	void parseTransaction(Session<Long, Long> session, LittleEndianDataInputStream in) {
		var txn = history.addTransaction(session, transactionId++);

		var length = in.readLong();
		for (long i = 0; i < length; i++) {
			var write = in.readBoolean();
			var key = in.readLong();
			var value = in.readLong();
			var success = in.readBoolean();

			if (!success) {
				throw new InvalidHistoryError();
			}
			history.addEvent(txn, write ? WRITE: READ, key, value);
		}

		var success = in.readBoolean();
		if (!success) {
			throw new InvalidHistoryError();
		}
	}
}
