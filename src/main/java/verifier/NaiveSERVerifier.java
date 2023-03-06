package verifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import history.History;
import history.HistoryLoader;
import history.Transaction;
import history.Event.EventType;

public class NaiveSERVerifier<KeyType, ValueType> {
    private final History<KeyType, ValueType> history;

    public NaiveSERVerifier(HistoryLoader<KeyType, ValueType> loader) {
        history = loader.loadHistory();
    }

    public boolean audit() {
        var transactions = (Transaction<KeyType, ValueType>[]) history.getTransactions().toArray(Transaction[]::new);
        return auditPermutation(transactions, new ArrayList<>(), new HashSet<>(), new HashMap<>());
    }

    private boolean auditPermutation(Transaction<KeyType, ValueType>[] allTransactions, ArrayList<Integer> permutation,
            HashSet<Integer> prefix, HashMap<KeyType, List<ValueType>> writes) {
        if (permutation.size() == allTransactions.length) {
            return true;
        }

        for (int i = 0; i < allTransactions.length; i++) {
            if (prefix.contains(i)) {
                continue;
            }

            permutation.add(i);
            prefix.add(i);

            var events = allTransactions[permutation.get(permutation.size() - 1)].getEvents();
            var reads = events.stream().filter(e -> e.getType() == EventType.READ)
                    .map(e -> Pair.of(e.getKey(), e.getValue()));

            if (reads.anyMatch(p -> {
                if (!writes.containsKey(p.getKey())) {
                    return true;
                }
                var values = writes.get(p.getKey());
                return values.get(values.size() - 1).equals(p.getValue());
            })) {
                continue;
            }

            var writesInThisTxn = events.stream().filter(e -> e.getType() == EventType.WRITE)
                    .map(e -> Pair.of(e.getKey(), e.getValue())).collect(Collectors.toList());
            for (var p : writesInThisTxn) {
                writes.computeIfAbsent(p.getKey(), k -> new ArrayList<>()).add(p.getValue());
            }

            if (auditPermutation(allTransactions, permutation, prefix, writes)) {
                return true;
            }

            for (var p : writesInThisTxn) {
                var list = writes.get(p.getKey());
                list.remove(list.size() - 1);
            }
            permutation.remove(permutation.size() - 1);
            prefix.remove(i);
        }
        return false;
    }
}
