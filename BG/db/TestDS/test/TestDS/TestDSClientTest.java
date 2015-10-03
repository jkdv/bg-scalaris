package TestDS;

import com.google.gson.*;
import de.zib.scalaris.ErlangValue;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.StringByteIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TestDSClient.class, TransactionHelper.class})
public class TestDSClientTest {
    private static final String FRIEND_COUNT = "friendcount";
    private static final String RESOURCE_COUNT = "resourcecount";
    private static final String PENDING_COUNT = "pendingcount";

    TestDSClient testDSClient;
    @Mock
    TransactionHelper transactionHelper;
    JsonObject jsonObject;

    @Before
    public void setUp() throws Exception {
        JsonParser jsonParser = new JsonParser();
        jsonObject = jsonParser.parse("{\"userid\":\"\",\"username\":\"\",\"pw\":\"\",\"fname\":\"\",\"lname\":\"\"," +
                "\"gender\":\"\"," +
                "\"dob\":\"\",\"jdate\":\"\",\"ldate\":\"\",\"address\":\"\",\"email\":\"\",\"tel\":\"\"}")
                .getAsJsonObject();
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());

        whenNew(TransactionHelper.class).withAnyArguments().thenReturn(transactionHelper);
        doNothing().when(transactionHelper).writeUser(anyString(), any(JsonObject.class));
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());

        testDSClient = new TestDSClient();
        testDSClient.init();
    }

    @Test
    public void testHashMapToGson() throws Exception {
        final HashMap<String, ByteIterator> forType = new HashMap<>();
        final Gson gson = new GsonBuilder()
                .registerTypeAdapter(forType.getClass(), new JsonSerializer<HashMap<String, ByteIterator>>() {
                    @Override
                    public JsonElement serialize(HashMap<String, ByteIterator> hashMap, Type type,
                                                 JsonSerializationContext context) {
                        JsonObject jsonObject = new JsonObject();
                        hashMap.forEach((k, v) -> jsonObject.add(k, new JsonPrimitive(v.toString())));
                        return jsonObject;
                    }
                }).create();

        ByteIterator byteIterator = new StringByteIterator("test");
        HashMap<String, ByteIterator> values = new HashMap<>();
        values.put("a", byteIterator);

        String jsonString = gson.toJson(values);
        assertThat(jsonString, is("{\"a\":\"test\"}"));

        JsonParser jsonParser = new JsonParser();
        JsonElement jsonElement = jsonParser.parse(jsonString);
        assertThat(jsonElement.toString(), is("{\"a\":\"test\"}"));
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        assertThat(jsonObject.toString(), is("{\"a\":\"test\"}"));
        JsonElement pendingFriendsElement = jsonObject.get("pendingFriends");
        assertThat(pendingFriendsElement, nullValue());
        JsonArray jsonArray = jsonObject.getAsJsonArray("pendingFriends");
        assertThat(jsonArray, nullValue());
    }

    @Test
    public void testErlangValue() throws Exception {
        String jsonString = "{\"a\":\"test\"}";
        ErlangValue erlangValue = new ErlangValue(jsonString);
        assertThat(erlangValue.stringValue(), is(jsonString));
    }

    @Test
    public void testInsertEntity() throws Exception {
        String entitySet = "users";
        String entityPK = "1";
        HashMap<String, ByteIterator> values = new HashMap<>();
        values.put("userid", new StringByteIterator(""));
        values.put("username", new StringByteIterator(""));
        values.put("pw", new StringByteIterator(""));
        values.put("fname", new StringByteIterator(""));
        values.put("lname", new StringByteIterator(""));
        values.put("gender", new StringByteIterator(""));
        values.put("dob", new StringByteIterator(""));
        values.put("jdate", new StringByteIterator(""));
        values.put("ldate", new StringByteIterator(""));
        values.put("address", new StringByteIterator(""));
        values.put("email", new StringByteIterator(""));
        values.put("tel", new StringByteIterator(""));

        int result = testDSClient.insertEntity(entitySet, entityPK, values, false);
        assertThat(result, is(0));

        entitySet = "resource";
        values.put("mid", new StringByteIterator(""));
        values.put("creatorid", new StringByteIterator(""));
        values.put("rid", new StringByteIterator(""));
        values.put("modifierid", new StringByteIterator(""));
        values.put("timestamp", new StringByteIterator(""));
        values.put("type", new StringByteIterator(""));
        values.put("content", new StringByteIterator(""));

        result = testDSClient.insertEntity(entitySet, entityPK, values, false);
        assertThat(result, is(0));
    }

    @Test
    public void testViewProfile() throws Exception {
        int requesterID = 1;
        int profileOwnerID = 1;
        HashMap<String, ByteIterator> result = new HashMap<>();

        JsonParser jsonParser = new JsonParser();
        jsonObject = jsonParser.parse("{\"userid\":\"\",\"username\":\"\",\"pw\":\"\",\"fname\":\"\",\"lname\":\"\",\"gender\":\"\"," +
                "\"dob\":\"\",\"jdate\":\"\",\"ldate\":\"\",\"address\":\"\",\"email\":\"\",\"tel\":\"\"," +
                "\"confirmedFriends\":[\"3\",\"4\"],\"pendingFriends\":[\"5\"],\"resources\":[\"1\"]}")
                .getAsJsonObject();
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());

        testDSClient.viewProfile(requesterID, profileOwnerID, result, false, false);
        assertTrue(result.containsKey(FRIEND_COUNT));
        assertTrue(result.containsKey(RESOURCE_COUNT));
        assertTrue(result.containsKey(PENDING_COUNT));

        jsonObject = jsonParser.parse("{\"userid\":\"\",\"username\":\"\",\"pw\":\"\",\"fname\":\"\",\"lname\":\"\"," +
                "\"gender\":\"\"," +
                "\"dob\":\"\",\"jdate\":\"\",\"ldate\":\"\",\"address\":\"\",\"email\":\"\",\"tel\":\"\"," +
                "\"confirmedFriends\":[],\"pendingFriends\":[\"5\"],\"resources\":[]}")
                .getAsJsonObject();
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());

        testDSClient.viewProfile(requesterID, profileOwnerID, result, false, false);
        assertTrue(result.containsKey(FRIEND_COUNT));
        assertTrue(result.containsKey(RESOURCE_COUNT));
        assertTrue(result.containsKey(PENDING_COUNT));

        jsonObject = jsonParser.parse("{\"userid\":\"\",\"username\":\"\",\"pw\":\"\",\"fname\":\"\",\"lname\":\"\"," +
                "\"gender\":\"\"," +
                "\"dob\":\"\",\"jdate\":\"\",\"ldate\":\"\",\"address\":\"\",\"email\":\"\",\"tel\":\"\"," +
                "\"confirmedFriends\":[\"3\",\"4\"],\"pendingFriends\":[],\"resources\":[]}")
                .getAsJsonObject();
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());

        testDSClient.viewProfile(requesterID, profileOwnerID, result, false, false);
        assertTrue(result.containsKey(FRIEND_COUNT));
        assertTrue(result.containsKey(RESOURCE_COUNT));
        assertTrue(result.containsKey(PENDING_COUNT));
    }

    @Test
    public void testListFriends() throws Exception {
        int requesterID = 1;
        int profileOwnerID = 1;
        Vector<HashMap<String, ByteIterator>> result = new Vector<>();
        int exitCode = testDSClient.listFriends(requesterID, profileOwnerID, null, result, false, false);

        result.forEach(hashMap -> {
            jsonObject.entrySet().forEach(entry -> {
                assertTrue(hashMap.containsKey(entry.getKey()));
            });
        });
        assertThat(exitCode, is(0));

        Set<String> fields = new HashSet<>();
        fields.add("userid");
        fields.add("username");

        exitCode = testDSClient.listFriends(requesterID, profileOwnerID, fields, result, false, false);

        result.forEach(hashMap -> {
            assertTrue(hashMap.containsKey("userid"));
            assertTrue(hashMap.containsKey("username"));
            assertFalse(hashMap.containsKey("fname"));
            assertFalse(hashMap.containsKey("lname"));
            assertFalse(hashMap.containsKey("gender"));
        });
        assertThat(exitCode, is(0));
    }

    @Test
    public void testViewFriendReq() throws Exception {
        int profileOwnerID = 1;
        Vector<HashMap<String, ByteIterator>> result = new Vector<>();
        int exitCode = testDSClient.viewFriendReq(profileOwnerID, result, false, false);

        result.forEach(hashMap -> {
            assertTrue(hashMap.containsKey("userid"));
            assertTrue(hashMap.containsKey("username"));
            assertTrue(hashMap.containsKey("fname"));
            assertTrue(hashMap.containsKey("lname"));
            assertTrue(hashMap.containsKey("gender"));
            assertTrue(hashMap.containsKey("dob"));
            assertTrue(hashMap.containsKey("jdate"));
            assertTrue(hashMap.containsKey("ldate"));
            assertTrue(hashMap.containsKey("address"));
            assertTrue(hashMap.containsKey("email"));
            assertTrue(hashMap.containsKey("tel"));
        });
        assertThat(exitCode, is(0));
    }

    @Test
    public void testAcceptFriend() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testRejectFriend() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testInviteFriend() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testViewTopKResources() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testGetCreatedResources() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testViewCommentOnResource() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testPostCommentOnResource() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testDelCommentOnResource() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testThawFriendship() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testGetInitialStats() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testCreateFriendship() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testCreateSchema() throws Exception {
        assertTrue(true);
    }

    @Test
    public void testQueryPendingFriendshipIds() throws Exception {
        Vector<Integer> result = new Vector<>();
        testDSClient.queryPendingFriendshipIds(1, result);
        assertThat(result.size(), is(0));

        JsonParser jsonParser = new JsonParser();
        jsonObject = jsonParser.parse("{\"userid\":\"\",\"username\":\"\",\"pw\":\"\",\"fname\":\"\",\"lname\":\"\",\"gender\":\"\"," +
                "\"dob\":\"\",\"jdate\":\"\",\"ldate\":\"\",\"address\":\"\",\"email\":\"\",\"tel\":\"\"," +
                "\"confirmedFriends\":[\"3\",\"4\"],\"pendingFriends\":[\"5\"],\"resources\":[\"1\"]}")
                .getAsJsonObject();
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());
        testDSClient.queryPendingFriendshipIds(1, result);
        assertThat(result.size(), is(1));
        assertTrue(result.contains(5));
    }

    @Test
    public void testQueryConfirmedFriendshipIds() throws Exception {
        Vector<Integer> result = new Vector<>();
        testDSClient.queryConfirmedFriendshipIds(1, result);
        assertThat(result.size(), is(0));

        JsonParser jsonParser = new JsonParser();
        jsonObject = jsonParser.parse("{\"userid\":\"\",\"username\":\"\",\"pw\":\"\",\"fname\":\"\",\"lname\":\"\",\"gender\":\"\"," +
                "\"dob\":\"\",\"jdate\":\"\",\"ldate\":\"\",\"address\":\"\",\"email\":\"\",\"tel\":\"\"," +
                "\"confirmedFriends\":[\"3\",\"4\"],\"pendingFriends\":[\"5\"],\"resources\":[\"1\"]}")
                .getAsJsonObject();
        doReturn(jsonObject).when(transactionHelper).readUser(anyString());
        testDSClient.queryConfirmedFriendshipIds(1, result);
        assertThat(result.size(), is(2));
        assertTrue(result.contains(3));
        assertTrue(result.contains(4));
    }
}