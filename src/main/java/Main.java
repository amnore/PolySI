import java.nio.file.Path;
import java.util.concurrent.Callable;

import history.HistoryLoader;
import history.HistoryParser;
import history.loaders.CobraHistoryLoader;
import history.loaders.DBCopHistoryLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import util.Profiler;
import util.UnimplementedError;
import verifier.SIVerifier;

@Command(name = "verifier", mixinStandardHelpOptions = true, version = "verifier 0.0.1", subcommands = { Audit.class,
		Convert.class })
public class Main implements Callable<Integer> {
	public static void main(String[] args) {
		var cmd = new CommandLine(new Main());
		cmd.setCaseInsensitiveEnumValuesAllowed(true);
		System.exit(cmd.execute(args));
	}

	@Override
	public Integer call() {
		CommandLine.usage(this, System.err);
		return -1;
	}
}

@Command(name = "audit", mixinStandardHelpOptions = true)
class Audit implements Callable<Integer> {
	@Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
	private final HistoryType type = HistoryType.COBRA;

	@Parameters(description = "history path")
	private Path path;

	private final Profiler profiler = Profiler.getInstance();

	@Override
	public Integer call() {
		HistoryLoader<?, ?> loader;
		switch (type) {
		case COBRA:
			loader = new CobraHistoryLoader(path);
			break;
		case DBCOP:
			loader = new DBCopHistoryLoader(path);
			break;
		default:
			throw new Error();
		}

		profiler.startTick("ENTIRE_EXPERIMENT");
		var pass = true;
		try {
			var verifier = new SIVerifier<>(loader);

			pass = verifier.audit();
		} catch (Throwable e) {
			pass = false;
			e.printStackTrace();
		}

		profiler.endTick("ENTIRE_EXPERIMENT");
		for (var p : profiler.getDurations()) {
			System.err.printf("%s: %dms\n", p.getKey(), p.getValue());
		}

		if (pass) {
			System.err.println("[[[[ ACCEPT ]]]]");
			return 0;
		} else {
			System.err.println("[[[[ REJECT ]]]]");
			return -1;
		}
	}
}

@Command(name = "convert", mixinStandardHelpOptions = true)
class Convert implements Callable<Integer> {
	@Option(names = { "-f", "--from" }, description = "input history type: ${COMPLETION-CANDIDATES}")
	private final HistoryType inType = HistoryType.COBRA;

	@Option(names = { "-o", "--output" }, description = "input history type: ${COMPLETION-CANDIDATES}")
	private final HistoryType outType = HistoryType.DBCOP;

	@Parameters(description = "input history path", index = "0")
	private Path inPath;

	@Parameters(description = "output history path", index = "1")
	private Path outPath;

	@Override
	public Integer call() {
		HistoryParser in;
		HistoryParser out;

		switch (inType) {
		case COBRA:
			in = new CobraHistoryLoader(inPath);
			break;
		case DBCOP:
			in = new DBCopHistoryLoader(inPath);
			break;
		default:
			throw new UnimplementedError();
		}

		switch (outType) {
		case COBRA:
			out = new CobraHistoryLoader(outPath);
			break;
		case DBCOP:
			out = new DBCopHistoryLoader(outPath);
			break;
		default:
			throw new UnimplementedError();
		}

		var oldHistory = in.loadHistory();
		var internalHistory = in.toLongLongHistory(oldHistory);
		var newHistory = out.fromLongLongHistory(internalHistory);
		out.dumpHistory(newHistory);

		return 0;
	}

}

enum HistoryType {
	COBRA, DBCOP
}
