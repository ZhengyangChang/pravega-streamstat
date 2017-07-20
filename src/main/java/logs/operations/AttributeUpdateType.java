package logs.operations;

import io.pravega.common.util.EnumHelpers;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines a type of update for a particular Attribute.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum AttributeUpdateType {
    /**
     * Only allows setting the value if it has not already been set. If it is set, the update will fail.
     */
    None((byte) 0),

    /**
     * Replaces the current value of the attribute with the one given. If no value is currently set, it sets the value
     * to the given one.
     */
    Replace((byte) 1),

    /**
     * Any updates will replace the current attribute value, but only if the new value is greater than the current
     * value (or no value defined currently). This does not require the updates to be consecutive. For example,
     * if A and B (A &lt; B) are updated concurrently, odds are that B will make it but A won't - this will be observed
     * by either A failing or both succeeding, but in the end, the final result will contain B.
     */
    ReplaceIfGreater((byte) 2),

    /**
     * Replaces the current value of the attribute with the sum of the current value and the one given. If no value is
     * currently set, it sets the given value as the attribute value.
     */
    Accumulate((byte) 3),

    /**
     * Any updates will replace the current attribute value, but only if the existing value matches
     * an expected value. If the value is different the update will fail. This can be used to
     * perform compare and set operations. The value is used to indicate a non value. IE: to remove
     * the attribute or if the value is expected not to
     * exist.
     */
    ReplaceIfEquals((byte) 4);


    private static final AttributeUpdateType[] MAPPING = EnumHelpers.indexById(AttributeUpdateType.class, AttributeUpdateType::getTypeId);
    @Getter
    private final byte typeId;

    /**
     * Gets the AttributeUpdateType that has the given type id.
     *
     * @param typeId The type id to search by.
     * @return The mapped AttributeUpdateType, or null
     */
    public static AttributeUpdateType get(byte typeId) {
        if (typeId < 0 || typeId >= MAPPING.length || MAPPING[typeId] == null) {
            throw new IllegalArgumentException("Unsupported AttributeUpdateType Id " + typeId);
        }

        return MAPPING[typeId];
    }
}