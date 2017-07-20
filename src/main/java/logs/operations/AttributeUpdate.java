package logs.operations;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.UUID;

/**
 * Represents an update to a value of an Attribute.
 */
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@NotThreadSafe
public class AttributeUpdate {
    /**
     * The ID of the Attribute to update.
     */
    private final UUID attributeId;

    /**
     * The UpdateType of the attribute.
     */
    private final AttributeUpdateType updateType;

    /**
     * The new Value of the attribute.
     */
    private long value;

    /**
     * If UpdateType is ReplaceIfEquals, then this is the value that the attribute must currently have before making the
     * update. Otherwise this field is ignored.
     */
    private final long comparisonValue;

    @Override
    public String toString() {
        return String.format("AttributeId = %s, Value = %s, UpdateType = %s", this.attributeId, this.value, this.updateType);
    }
}