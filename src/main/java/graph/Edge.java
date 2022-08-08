package graph;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collection;

@Data
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Edge<KeyType> {
    @EqualsAndHashCode.Include
    private final EdgeType type;
    private final Collection<KeyType> keys;

    @Override
    public String toString() {
        return String.format("(%s, %s)", type, keys);
    }
}
