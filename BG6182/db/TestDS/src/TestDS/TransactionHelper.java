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
    private boolean isTransaction = false;
    private Transaction transaction;
    private Connection connection;
    private static final String USER_ID_PREFIX = "u";
    private static final String RESOURCE_ID_PREFIX = "r";
    private static final String MANIPULATION = "manipulation";
    private static final double INIT_WAIT_TIME = 5;
    private static final double MAX_WAIT_TIME = 10;

    /**
     * Creates an instance with connection pool.
     *
     * @param connectionPool A ConnectionPool instance.
     */
    public TransactionHelper(ConnectionPool connectionPool) {
        this.jsonParser = new JsonParser();
        this.connectionPool = connectionPool;
    }

    /**
     * Returns a connection from the given connection pool.
     *
     * @return An instance of Connection.
     */
    private Connection getConnection() {
        double ms = INIT_WAIT_TIME;
        Connection conn;
        while (true) {
            try {
                conn = connectionPool.getConnection();
                if (conn != null) break;
            } catch (ConnectionException e) {
                try {
                    Thread.sleep((long) ms);
                    if (ms < MAX_WAIT_TIME)
                        ms *= Math.log(ms);
                } catch (InterruptedException ignored) {
                }
            }
        }
        return conn;
    }

    /**
     * Begins a transaction.
     */
    public void beginTransaction() {
        //isTransaction = true;
        //connection = getConnection();
        //transaction = new Transaction(connection);
    }

    /**
     * Ends a current transaction.
     */
    public void endTransaction() {
        //isTransaction = false;
        //connectionPool.releaseConnection(connection);
    }

    /**
     * Read a user value as a JSON object.
     *
     * @param userId User ID given by BG.
     * @return JsonObject instance.
     * @throws NotFoundException
     */
    public JsonObject readUser(final String userId) throws NotFoundException {
        return read(String.format("%s%s", USER_ID_PREFIX, userId));
    }

    /**
     * Write a user value as a JSON object. A key will be the given user ID.
     *
     * @param userId User ID given by BG.
     * @param value  JsonObject instance.
     */
    public void writeUser(final String userId, final JsonObject value) {
        write(String.format("%s%s", USER_ID_PREFIX, userId), value);
    }

    /**
     * Read a resource value as a JSON obejct.
     *
     * @param resourceId Resource ID given by BG.
     * @return JsonObject instance.
     * @throws NotFoundException
     */
    public JsonObject readResource(final String resourceId) throws NotFoundException {
        return read(String.format("%s%s", RESOURCE_ID_PREFIX, resourceId));
    }

    /**
     * Write a resource value as a JSON Obejct.
     *
     * @param resourceId Resource ID given by BG.
     * @param value      JsonObject instance.
     */
    public void writeResource(final String resourceId, final JsonObject value) {
        write(String.format("%s%s", RESOURCE_ID_PREFIX, resourceId), value);
    }

    /**
     * Returns a manipulation value as a JSON object.
     *
     * @param resourceId Resource ID given by BG.
     * @return JsonObject instance.
     * @throws NotFoundException
     */
    public JsonObject readManipulations(final String resourceId) throws NotFoundException {
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
     * @throws NotFoundException
     */
    public void writeManipulation(final String resourceId, final String manipulationId, final JsonObject value)
            throws NotFoundException {
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
     * @throws NotFoundException
     */
    public void deleteManipulation(final String resourceId, final String manipulationId) throws NotFoundException {
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
     * @throws NotFoundException
     */
    private JsonObject read(final String key) throws NotFoundException {
        if (isTransaction) {
            return read(key, transaction);
        } else {
            Connection connection = getConnection();
            TransactionSingleOp transaction = new TransactionSingleOp(connection);

            JsonObject json = read(key, transaction);
            connectionPool.releaseConnection(connection);
            return json;
        }
    }

    /**
     * Read a value with the key.
     *
     * @param key Unique key.
     * @return JsonObject instance.
     * @throws NotFoundException
     */
    private JsonObject read(final String key,
                            AbstractTransaction<? extends RequestList, ? extends ResultList> transaction)
            throws NotFoundException {
        ErlangValue erlangValue;
        double ms = INIT_WAIT_TIME;
        while (true) {
            try {
                erlangValue = transaction.read(key);
                if (erlangValue != null) break;
            } catch (ConnectionException e) {
                try {
                    Thread.sleep((long) ms);
                    if (ms < MAX_WAIT_TIME)
                        ms *= Math.log(ms);
                } catch (InterruptedException ignored) {
                }
            }
        }
        JsonElement jsonElement = jsonParser.parse(erlangValue.stringValue());
        return jsonElement.getAsJsonObject();
    }

    /**
     * Write a value with the key.
     *
     * @param key   Unique key.
     * @param value JsonObject instance.
     */
    private void write(final String key, final JsonObject value) {
        if (isTransaction) {
            write(key, value, transaction);
        } else {
            Connection connection = getConnection();
            TransactionSingleOp transaction = new TransactionSingleOp(connection);
            write(key, value, transaction);
            connectionPool.releaseConnection(connection);
        }
    }

    /**
     * Write a value with the key.
     *
     * @param key   Unique key.
     * @param value JsonObject instance.
     */
    private void write(final String key, final JsonObject value,
                       AbstractTransaction<? extends RequestList, ? extends ResultList> transaction) {
        double ms = INIT_WAIT_TIME;
        while (true) {
            try {
                transaction.write(key, value.toString());
                break;
            } catch (ConnectionException | AbortException e) {
                try {
                    Thread.sleep((long) ms);
                    if (ms < MAX_WAIT_TIME)
                        ms *= Math.log(ms);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
