import java.nio.file.Path;
import java.util.concurrent.Callable;

import graph.CobraHistoryLoader;
import graph.DBCopHistoryLoader;
import graph.HistoryLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import util.Profiler;
import verifier.SIVerifier;

@Command(name = "verifier", mixinStandardHelpOptions = true, version = "verifier 0.0.1", subcommands = { Audit.class, Cobra.class })
public class Main implements Callable<Integer> {
	public static void main(String[] args) {
		var cmd = new CommandLine(new Main());
		cmd.setCaseInsensitiveEnumValuesAllowed(true);
		System.exit(cmd.execute(args));
	}

	@Override
	public Integer call() throws Exception {
		CommandLine.usage(this, System.err);
		return -1;
	}
}

@Command(name = "cobra")
class Cobra implements Callable<Integer> {
	@Parameters(index = "0..*")
	private String[] args = {};

	@Override
	public Integer call() {
		test.Main.main(args);
		return 0;
	}
}

@Command(name = "audit", mixinStandardHelpOptions = true)
class Audit implements Callable<Integer> {
	@Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
	private HistoryType type = HistoryType.COBRA;

	@Parameters(description = "history path")
	private Path path;

	private final Profiler profiler = Profiler.getInstance();

	@Override
	public Integer call() {
		HistoryLoader loader;
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
			profiler.startTick("ONESHOT_CONS");
			var verifier = new SIVerifier(loader);
			profiler.endTick("ONESHOT_CONS");

			profiler.startTick("ONESHOT_SOLVE");
			pass = verifier.audit();
			profiler.endTick("ONESHOT_SOLVE");
		} catch (Throwable e) {
			pass = false;
			e.printStackTrace();
		}

		profiler.endTick("ENTIRE_EXPERIMENT");
		System.err.printf(">>> Overall runtime = %dms\n  construct: %dms\n  solve: %dms\n",
				profiler.getTime("ENTIRE_EXPERIMENT"), profiler.getTime("ONESHOT_CONS"),
				profiler.getTime("ONESHOT_SOLVE"));

		if (pass) {
			System.err.println("[[[[ ACCEPT ]]]]");
			return 0;
		} else {
			System.err.println("[[[[ REJECT ]]]]");
			return -1;
		}
	}

	enum HistoryType {
		COBRA, DBCOP
	}
}
