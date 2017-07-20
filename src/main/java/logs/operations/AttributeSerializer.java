package logs.operations;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Helper functions
 */
public class AttributeSerializer {
    public static Map<UUID, Long> deserialize(DataInputStream stream) throws IOException {
        short attributeCount = stream.readShort();
        Map<UUID, Long> attributes = new HashMap<>();
        for (int i = 0; i < attributeCount; i++) {
            long mostSigBits = stream.readLong();
            long leastSigBits = stream.readLong();
            long value = stream.readLong();
            attributes.put(new UUID(mostSigBits, leastSigBits), value);
        }

        return attributes;
    }

    static Collection<AttributeUpdate> deserializeUpdates(DataInputStream stream) throws IOException {
        short attributeCount = stream.readShort();
        Collection<AttributeUpdate> result;
        if (attributeCount > 0) {
            result = new ArrayList<>(attributeCount);
            for (int i = 0; i < attributeCount; i++) {
                long idMostSig = stream.readLong();
                long idLeastSig = stream.readLong();
                UUID attributeId = new UUID(idMostSig, idLeastSig);
                AttributeUpdateType updateType = AttributeUpdateType.get(stream.readByte());
                long value = stream.readLong();
                long comparisonValue = stream.readLong();
                result.add(new AttributeUpdate(attributeId, updateType, value, comparisonValue));
            }
        } else {
            result = Collections.emptyList();
        }

        return result;
    }
}
