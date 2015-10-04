package TestDS;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.zib.scalaris.*;

/**
 * Wrapper class of TransactionSingleOp.
 */
public class TransactionHelper {
    private final TransactionSingleOp transactionSingleOp;
    private final Transaction transaction;
    private JsonParser jsonParser;
    private static final String USER_ID_PREFIX = "u";
    private static final String RESOURCE_ID_PREFIX = "r";
    private static final String USER_LIST = "user_list";

    /**
     * Constructor.
     *
     * @throws ConnectionException
     */
    public TransactionHelper() throws ConnectionException {
        transactionSingleOp = new TransactionSingleOp();
        transaction = new Transaction();
        jsonParser = new JsonParser();
    }

    /**
     * Read a user value as a JSON object.
     *
     * @param userId User ID given by BG.
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    public JsonObject readUser(String userId) throws ConnectionException, NotFoundException {
        return read(String.format("%s%s", USER_ID_PREFIX, userId));
    }

    /**
     * Write a user value as a JSON object. A key will be the given user ID.
     *
     * @param userId User ID given by BG.
     * @param value  JsonObject instance.
     * @throws ConnectionException
     * @throws AbortException
     */
    public void writeUser(String userId, JsonObject value) throws ConnectionException, AbortException {
        write(String.format("%s%s", USER_ID_PREFIX, userId), value);
    }

    /**
     * Read a value with the key.
     *
     * @param key Unique key.
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    private JsonObject read(String key) throws ConnectionException, NotFoundException {
        ErlangValue erlangValue = transactionSingleOp.read(key);
        JsonElement jsonElement = jsonParser.parse(erlangValue.stringValue());
        return jsonElement.getAsJsonObject();
    }

    /**
     * Write a value with they key.
     * @param key Unique key.
     * @param value JsonObject instance.
     * @throws ConnectionException
     * @throws AbortException
     */
    private void write(String key, JsonObject value) throws ConnectionException, AbortException {
        transactionSingleOp.write(key, value.toString());
    }
}
