package TestDS;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.zib.scalaris.*;

/**
 * Wrapper class of TransactionSingleOp.
 */
public class TransactionHelper {
    private ConnectionPool connectionPool;
    private JsonParser jsonParser;
    private static final String USER_ID_PREFIX = "u";
    private static final String RESOURCE_ID_PREFIX = "r";
    private static final String USER_LIST = "user_list";
    private static final String MANIPULATION = "manipulation";

    /**
     * Creates an instance with connection pool.
     *
     * @param connectionPool A ConnectionPool instance.
     */
    public TransactionHelper(ConnectionPool connectionPool) {
        jsonParser = new JsonParser();
        this.connectionPool = connectionPool;
    }

    /**
     * Read a user value as a JSON object.
     *
     * @param userId User ID given by BG.
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    public JsonObject readUser(final String userId) throws ConnectionException, NotFoundException {
        return read(String.format("%s%s", USER_ID_PREFIX, userId));
    }

    /**
     * Returns a list of user IDs.
     *
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    public JsonObject readUserList() throws ConnectionException, NotFoundException {
        return read(USER_LIST);
    }

    /**
     * Write a user value as a JSON object. A key will be the given user ID.
     *
     * @param userId User ID given by BG.
     * @param value  JsonObject instance.
     * @throws ConnectionException
     * @throws AbortException
     */
    public void writeUser(final String userId, final JsonObject value) throws ConnectionException, AbortException {
        write(String.format("%s%s", USER_ID_PREFIX, userId), value);

        JsonObject userListObject;
        try {
            userListObject = read(USER_LIST);
        } catch (NotFoundException e) {
            userListObject = new JsonObject();
        }

        userListObject.addProperty(userId, userId);
        write(USER_LIST, userListObject);
    }

    /**
     * Read a resource value as a JSON obejct.
     *
     * @param resourceId Resource ID given by BG.
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    public JsonObject readResource(final String resourceId) throws ConnectionException, NotFoundException {
        return read(String.format("%s%s", RESOURCE_ID_PREFIX, resourceId));
    }

    /**
     * Write a resource value as a JSON Obejct.
     *
     * @param resourceId Resource ID given by BG.
     * @param value      JsonObject instance.
     * @throws ConnectionException
     * @throws AbortException
     */
    public void writeResource(final String resourceId, final JsonObject value) throws ConnectionException,
            AbortException {
        write(String.format("%s%s", RESOURCE_ID_PREFIX, resourceId), value);
    }

    /**
     * Returns a manipulation value as a JSON object.
     *
     * @param resourceId Resource ID given by BG.
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    public JsonObject readManipulations(final String resourceId) throws ConnectionException, NotFoundException {
        JsonObject manipulationObject;
        JsonObject resourceObject = readResource(resourceId);
        if (resourceObject.has(MANIPULATION)) {
            manipulationObject = resourceObject.getAsJsonObject(MANIPULATION);
        } else {
            manipulationObject = new JsonObject();
        }
        return manipulationObject;
    }

    /**
     * Add a manipulation value to the resource value.
     *
     * @param resourceId     Resource ID given by BG.
     * @param manipulationId Manipulation ID given by BG.
     * @param value          JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    public void writeManipulation(final String resourceId, final String manipulationId, final JsonObject value) throws
            ConnectionException, NotFoundException, AbortException {
        JsonObject manipulationObject;
        JsonObject resourceObject = readResource(resourceId);
        if (resourceObject.has(MANIPULATION)) {
            manipulationObject = resourceObject.getAsJsonObject(MANIPULATION);
        } else {
            manipulationObject = new JsonObject();
        }
        manipulationObject.add(manipulationId, value);
        resourceObject.add(MANIPULATION, manipulationObject);
        writeResource(resourceId, resourceObject);
    }

    /**
     * Delete the manipulation value from the resource.
     *
     * @param resourceId     Resource ID given by BG.
     * @param manipulationId Manipulation ID given by BG.
     * @throws ConnectionException
     * @throws NotFoundException
     * @throws AbortException
     */
    public void deleteManipulation(final String resourceId, final String manipulationId) throws ConnectionException,
            NotFoundException, AbortException {
        JsonObject resourceObject = readResource(resourceId);
        if (resourceObject.has(MANIPULATION)) {
            JsonObject manipulationObject = resourceObject.getAsJsonObject(MANIPULATION);
            manipulationObject.remove(manipulationId);
            resourceObject.add(MANIPULATION, manipulationObject);
            writeResource(resourceId, resourceObject);
        }
    }

    /**
     * Read a value with the key.
     *
     * @param key Unique key.
     * @return JsonObject instance.
     * @throws ConnectionException
     * @throws NotFoundException
     */
    private JsonObject read(final String key) throws NotFoundException, ConnectionException {
        Connection connection = null;

        for (int i = 1; i <= 1024; i *= 2) {
            try {
                connection = connectionPool.getConnection();

                break;
            } catch (ConnectionException e) {
                try {
                    Thread.sleep(i);
                } catch (InterruptedException ignored) {
                }
            }
        }

        if (connection == null) {
            throw new ConnectionException("Failed to retrieve connection from pool.");
        }

        TransactionSingleOp transactionSingleOp = new TransactionSingleOp(connection);
        ErlangValue erlangValue = null;

        for (int i = 1; i <= 1024; i *= 2) {
            try {
                erlangValue = transactionSingleOp.read(key);
            } catch (ConnectionException e) {
                try {
                    Thread.sleep(i);
                } catch (InterruptedException ignored) {
                }
            }
        }

        if (erlangValue == null) {
            throw new ConnectionException("Failed to retrieve connection from pool.");
        }

        JsonElement jsonElement = jsonParser.parse(erlangValue.stringValue());
        connectionPool.releaseConnection(connection);

        return jsonElement.getAsJsonObject();
    }

    /**
     * Write a value with they key.
     *
     * @param key   Unique key.
     * @param value JsonObject instance.
     * @throws ConnectionException
     * @throws AbortException
     */
    private void write(final String key, final JsonObject value) throws ConnectionException {
        Connection connection = null;

        for (int i = 1; i <= 10240; i *= 2) {
            try {
                connection = connectionPool.getConnection();

                break;
            } catch (ConnectionException e) {
                try {
                    Thread.sleep(i);
                } catch (InterruptedException ignored) {
                }
            }
        }

        if (connection == null) {
            throw new ConnectionException("Failed to retrieve connection from pool.");
        }

        TransactionSingleOp transactionSingleOp = new TransactionSingleOp(connection);

        for (int i = 1; i <= 10240; i *= 2) {
            try {
                transactionSingleOp.write(key, value.toString());
            } catch (AbortException e) {
                try {
                    Thread.sleep(i);
                } catch (InterruptedException ignored) {
                }
            }
        }

        connectionPool.releaseConnection(connection);
    }
}
