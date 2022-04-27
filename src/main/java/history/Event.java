package history;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class Event<KeyType, ValueType> {
	public enum EventType {
    	READ, WRITE
    }

    @EqualsAndHashCode.Include
	private final Transaction<KeyType, ValueType> transaction;

	@EqualsAndHashCode.Include
	@ToString.Include
	private final Event.EventType type;

	@EqualsAndHashCode.Include
	@ToString.Include
	private final KeyType key;

	@EqualsAndHashCode.Include
	@ToString.Include
	private final ValueType value;
}
