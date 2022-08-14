import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import history.Event;
import history.Event.EventType;
import history.History;
import history.HistoryLoader;
import history.HistoryParser;
import history.HistoryTransformer;
import history.loaders.CobraHistoryLoader;
import history.loaders.DBCopHistoryLoader;
import history.loaders.TextHistoryLoader;
import history.transformers.Identity;
import history.transformers.SnapshotIsolationToSerializable;
import lombok.SneakyThrows;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import util.Profiler;
import util.UnimplementedError;
import verifier.Pruning;
import verifier.SIVerifier;
import graph.MatrixGraph;

@Command(name = "verifier", mixinStandardHelpOptions = true, version = "verifier 0.0.1", subcommands = {
        Audit.class, Convert.class, Stat.class, Dump.class })
public class Main implements Callable<Integer> {
    @SneakyThrows
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
    @Option(names = { "-t",
            "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Option(names = { "--no-pruning" }, description = "disable pruning")
    private final Boolean noPruning = false;

    @Option(names = { "--no-coalescing" }, description = "disable coalescing")
    private final Boolean noCoalescing = false;

    @Parameters(description = "history path")
    private Path path;

    private final Profiler profiler = Profiler.getInstance();

    @Override
    public Integer call() {
        var loader = Utils.getParser(type, path);

        Pruning.setEnablePruning(!noPruning);
        SIVerifier.setCoalesceConstraints(!noCoalescing);

        profiler.startTick("ENTIRE_EXPERIMENT");
        var pass = true;
        var verifier = new SIVerifier<>(loader);
        pass = verifier.audit();
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
    @Option(names = { "-f",
            "--from" }, description = "input history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType inType = HistoryType.COBRA;

    @Option(names = { "-o",
            "--output" }, description = "input history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType outType = HistoryType.DBCOP;

    @Option(names = { "-t",
            "--transform" }, description = "history transformation: ${COMPLETION-CANDIDATES}")
    private final HistoryTransformation transformation = HistoryTransformation.IDENTITY;

    @Parameters(description = "input history path", index = "0")
    private Path inPath;

    @Parameters(description = "output history path", index = "1")
    private Path outPath;

    @Override
    public Integer call() {
        var in = Utils.getParser(inType, inPath);
        var out = Utils.getParser(outType, outPath);
        var transformer = Utils.getTransformer(transformation);

        var history = in.loadHistory();
        history = transformer.transformHistory(history);
        convertAndDump(out, history);

        return 0;
    }

    private <T, U> void convertAndDump(HistoryParser<T, U> parser,
            History<?, ?> history) {
        parser.dumpHistory(parser.convertFrom(history));
    }
}

@Command(name = "stat", mixinStandardHelpOptions = true)
class Stat implements Callable<Integer> {
    @Option(names = { "-t",
            "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Parameters(description = "history path")
    private Path path;

    @Override
    public Integer call() {
        var loader = Utils.getParser(type, path);
        var history = loader.loadHistory();

        var events = history.getEvents();
        var writeFreq = events.stream()
                .collect(Collectors.toMap(ev -> ev.getKey(),
                        ev -> ev.getType().equals(EventType.WRITE) ? 1 : 0,
                        Integer::sum))
                .entrySet().stream()
                .collect(Collectors.toMap(w -> w.getValue(), w -> 1,
                        Integer::sum))
                .entrySet().stream()
                .sorted((p, q) -> Integer.compare(p.getKey(), q.getKey()))
                .collect(Collectors.toCollection(ArrayList::new));

        System.out.printf("Sessions: %d\n"
                + "Transactions: %d, read-only: %d, write-only: %d\n"
                + "Events: total %d, read %d, write %d\n" + "Variables: %d\n",
                history.getSessions().size(), history.getTransactions().size(),
                history.getTransactions().stream()
                        .filter(txn -> txn.getEvents().stream()
                                .allMatch(ev -> ev.getType() == EventType.READ))
                        .count(),
                history.getTransactions().stream()
                        .filter(txn -> txn.getEvents().stream()
                                .allMatch(ev -> ev.getType() == EventType.WRITE))
                        .count(),
                events.size(),
                events.stream().filter(e -> e.getType() == Event.EventType.READ)
                        .count(),
                events.stream()
                        .filter(e -> e.getType() == Event.EventType.WRITE)
                        .count(),
                events.stream().map(e -> e.getKey()).distinct().count());

        System.out.println("(writes, #keys):");
        int min = writeFreq.get(0).getKey(),
                max = writeFreq.get(writeFreq.size() - 1).getKey();
        double start = writeFreq.size() == 1 ? writeFreq.get(0).getKey()
                : writeFreq.get(1).getKey(), step = (max - min) / 8.0;
        int count = 0;
        writeFreq.add(Pair.of(
                writeFreq.get(writeFreq.size() - 1).getKey() + (int) step + 1,
                0));
        System.out.printf("%d: %d\n", writeFreq.get(0).getKey(),
                writeFreq.get(0).getValue());
        for (int i = 2; i < writeFreq.size(); i++) {
            var p = writeFreq.get(i);
            if (p.getKey() <= start + step) {
                count += p.getValue();
            } else {
                System.out.printf("%.0f...%.0f: %d\n", Math.ceil(start),
                        Math.floor(start + step), count);
                count = p.getValue();
                while (start + step < p.getKey()) {
                    start += step;
                }
            }
        }

        return 0;
    }
}

@Command(name = "dump", mixinStandardHelpOptions = true)
class Dump implements Callable<Integer> {
    @Option(names = { "-t",
            "--type" }, description = "history type: ${COMPLETION-CANDIDATES}")
    private final HistoryType type = HistoryType.COBRA;

    @Parameters(description = "history path")
    private Path path;

    @Override
    public Integer call() {
        var loader = Utils.getParser(type, path);
        var history = loader.loadHistory();

        for (var session : history.getSessions()) {
            for (var txn : session.getTransactions()) {
                var events = txn.getEvents();
                System.out.printf("Transaction %s\n", txn);
                for (var j = 0; j < events.size(); j++) {
                    var ev = events.get(j);
                    System.out.printf("%s\n", ev);
                }
                System.out.println();
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
        case TEXT:
            return new TextHistoryLoader(path);
        default:
            throw new UnimplementedError();
        }

    }

    static HistoryTransformer getTransformer(HistoryTransformation transform) {
        switch (transform) {
        case IDENTITY:
            return new Identity();
        case SI2SER:
            return new SnapshotIsolationToSerializable();
        default:
            throw new UnimplementedError();
        }
    }
}

enum HistoryType {
    COBRA, DBCOP, TEXT
}

enum HistoryTransformation {
    IDENTITY, SI2SER
}
