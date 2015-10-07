package TestDS;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import edu.usc.bg.base.ByteArrayByteIterator;
import edu.usc.bg.base.ObjectByteIterator;

import java.util.Base64;

public final class ImageUtils {

    public static JsonPrimitive toJsonPrimitive(final byte[] bytes) {
        return new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
    }

    public static ObjectByteIterator toObejctByteIterator(final JsonElement jsonElement) {
        return new ObjectByteIterator(Base64.getDecoder().decode(jsonElement.getAsString()));
    }
}
