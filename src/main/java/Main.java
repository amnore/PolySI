import java.nio.file.Path;
import java.util.concurrent.Callable;

import history.Event;
import history.Event.EventType;
import history.History;
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
        Convert.class, Stat.class, Dump.class })
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
        var loader = Utils.getParser(type, path);

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
        var in = Utils.getParser(inType, inPath);
        var out = Utils.getParser(outType, outPath);

        var oldHistory = in.loadHistory();
        convertAndDump(out, oldHistory);

        return 0;
    }

    private <T, U> void convertAndDump(HistoryParser<T, U> parser, History<?, ?> history) {
        parser.dumpHistory(parser.convertFrom(history));
    }
}

@Command(name = "stat", mixinStandardHelpOptions = true)
class Stat implements Callable<Integer> {
    @Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Parameters(description = "history path")
    private Path path;

    @Override
    public Integer call() {
        var loader = Utils.getParser(type, path);
        var history = loader.loadHistory();

        var events = history.getEvents();
        System.out.printf(
                "Sessions: %d\n" + "Transactions: %d\n" + "Events: total %d, read %d, write %d\n" + "Variables: %d\n",
                history.getSessions().size(), history.getTransactions().size(), events.size(),
                events.stream().filter(e -> e.getType() == Event.EventType.READ).count(),
                events.stream().filter(e -> e.getType() == Event.EventType.WRITE).count(),
                events.stream().map(e -> e.getKey()).distinct().count());

        return 0;
    }
}

@Command(name = "dump", mixinStandardHelpOptions = true)
class Dump implements Callable<Integer> {
    @Option(names = { "-t", "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Parameters(description = "history path")
    private Path path;

    @Override
    public Integer call() {
        var loader = Utils.getParser(type, path);
        var history = loader.loadHistory();

        for (var session : history.getSessions()) {
            var txns = session.getTransactions();
            for (var i = 0; i < txns.size(); i++) {
                var events = txns.get(i).getEvents();
                for (var j = 0; j < events.size(); j++) {
                    var ev = events.get(j);
                    System.out.printf("%c(%s, %s, %d, %d, %d)\n", ev.getType() == Event.EventType.WRITE ? 'W' : 'R',
                            ev.getKey(), ev.getValue(), session.getId(), i, j);
                }
            }
        }

        return 0;
    }

}

class Utils {
    static HistoryParser<?, ?> getParser(HistoryType type, Path path) {
        switch (type) {
            case COBRA:
                return new CobraHistoryLoader(path);
            case DBCOP:
                return new DBCopHistoryLoader(path);
            default:
                throw new UnimplementedError();
        }

    }
}

enum HistoryType {
    COBRA, DBCOP
}
