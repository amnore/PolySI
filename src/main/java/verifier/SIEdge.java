package verifier;

import graph.EdgeType;
import history.Transaction;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collection;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
class SIEdge<KeyType, ValueType> {
    @EqualsAndHashCode.Include
    private final Transaction<KeyType, ValueType> from;
    @EqualsAndHashCode.Include
    private final Transaction<KeyType, ValueType> to;
    @EqualsAndHashCode.Include
    private final EdgeType type;
    private final Collection<KeyType> keys;

    @Override
    public String toString() {
        return String.format("(%s -> %s, %s, %s)", from, to, type, keys);
    }
}
