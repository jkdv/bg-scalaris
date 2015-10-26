package TestDS;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.zib.scalaris.*;
import edu.usc.bg.base.*;

import java.util.*;

public class TestDSClient extends DB {
    private TransactionHelper transactionHelper;
    private ConnectionPool connectionPool;

    private static final String PENDING_FRIENDS = "pendingFriends";
    private static final String CONFIRMED_FRIENDS = "confirmedFriends";
    private static final String FRIEND_COUNT = "friendcount";
    private static final String RESOURCE_COUNT = "resourcecount";
    private static final String PENDING_COUNT = "pendingcount";
    private static final String USER_COUNT = "usercount";
    private static final String AVG_FRIENDS_PER_USER = "avgfriendsperuser";
    private static final String AVG_PENDING_PER_USER = "avgpendingperuser";
    private static final String RESOURCES_PER_USER = "resourcesperuser";
    private static final String USERS = "users";
    private static final String RESOURCES = "resources";
    private static final String WALL_USER_ID = "walluserid";
    private static final String CREATOR_ID = "creatorid";
    private static final String CREATED_RESOURCES = "createdResources";
    private static final String PIC = "pic";
    private static final String TPIC = "tpic";

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread. This
     * method should be called once by any thread to start communication with the database. The code written for this
     * function should initiate the thread's communication with the database.
     *
     * @return true if the connection to the data store was successful.
     */
    @Override
    public synchronized boolean init() throws DBException {
        connectionPool = new ConnectionPool(new ConnectionFactory(), 0);
        transactionHelper = new TransactionHelper(connectionPool);
        return super.init();
    }

    /**
     * Cleanup any state for this DB.
     *
     * @param warmup This flag identifies if the thread calling it is in the warm up phase. In the warm up phase the
     *               threads do not issue updates, this phase can be used for warming up caches. If the warm up is set
     *               to true, the cache would not be restarted at the end of the warmup phase and only the warm up
     *               thread's connections to the database will be recycled. Called once per DB instance; there is one DB
     *               instance per client thread. This method should be called once the thread needs to end its
     *               communication with the data store. The code written for this function should close up the
     *               connection of the thread with the database and clean up the database instance.
     */
    @Override
    public synchronized void cleanup(boolean warmup) throws DBException {
        connectionPool.closeAll();
        super.cleanup(warmup);
    }

    /**
     * This function is called in the load phase which is executed using the -load or -loadindex argument. It is used
     * for inserting users and resources. Any field/value pairs in the values HashMap for an entity will be written into
     * the specified entity set with the specified entity key.
     *
     * @param entitySet   The name of the entity set with the following two possible values:  users and resources. BG
     *                    passes these values in lower case.  The implementation may manipulate the case to tailor it
     *                    for the purposes of a data store.
     * @param entityPK    The primary key of the entity to insert.
     * @param values      A HashMap of field/value pairs to insert for the entity, these pairs are the other attributes
     *                    for an entity and their values. The profile image is identified with the "pic" key attribute
     *                    and the thumbnail image is identified with the "tpic" key attribute.
     * @param insertImage Identifies if images should be inserted for users. if set to true the code should populate
     *                    each entity with an image; the size of the image is specified using the imagesize parameter.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes. The code written for this function call should insert the entity and its attributes. The code is
     * responsible for inserting the PK and the other attributes in the appropriate order.
     */
    @Override
    public synchronized int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
                                         boolean insertImage) {
        /**
         * Insert Users and Resources data using JSON-like data model.
         */
        JsonObject jsonObject = new JsonObject();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            if (!entry.getKey().equals(PIC) && !entry.getKey().equals(TPIC)) {
                jsonObject.add(entry.getKey(), new JsonPrimitive(entry.getValue().toString()));
            } else if (insertImage) {
                byte[] byteImage = entry.getValue().toArray();
                jsonObject.add(entry.getKey(), ImageUtils.toJsonPrimitive(byteImage));
            }
        }

        if (USERS.equals(entitySet)) {
            try {
                transactionHelper.writeUser(entityPK, jsonObject);
                //transactionHelper.writeUserList(entityPK);
            } catch (ConnectionException | AbortException e) {
                e.printStackTrace();
                return -1;
            }
        }

        /**
         * Update Users data after inserting Resources.
         */
        else if (RESOURCES.equals(entitySet)) {
            try {
                transactionHelper.writeResource(entityPK, jsonObject);

                ByteIterator wallUserId = values.get(WALL_USER_ID);
                JsonObject wallUserObject = transactionHelper.readUser(wallUserId.toString());

                JsonArray resourceArray;
                if (wallUserObject.has(RESOURCES)) {
                    resourceArray = wallUserObject.getAsJsonArray(RESOURCES);
                } else {
                    resourceArray = new JsonArray();
                }

                resourceArray.add(new JsonPrimitive(entityPK));
                wallUserObject.add(RESOURCES, resourceArray);
                transactionHelper.writeUser(wallUserId.toString(), wallUserObject);

                /**
                 * Update createdResource of User data.
                 */
                ByteIterator creatorId = values.get(CREATOR_ID);

                if (wallUserId.equals(creatorId)) {
                    JsonArray createdResourceArray;
                    if (wallUserObject.has(CREATED_RESOURCES)) {
                        createdResourceArray = wallUserObject.getAsJsonArray(CREATED_RESOURCES);
                    } else {
                        createdResourceArray = new JsonArray();
                    }

                    createdResourceArray.add(new JsonPrimitive(entityPK));
                    wallUserObject.add(CREATED_RESOURCES, createdResourceArray);
                    transactionHelper.writeUser(wallUserId.toString(), wallUserObject);
                } else {
                    JsonObject creatorObject = transactionHelper.readUser(creatorId.toString());
                    JsonArray createdResourceArray;

                    if (creatorObject.has(CREATED_RESOURCES)) {
                        createdResourceArray = creatorObject.getAsJsonArray(CREATED_RESOURCES);
                    } else {
                        createdResourceArray = new JsonArray();
                    }

                    createdResourceArray.add(new JsonPrimitive(entityPK));
                    creatorObject.add(CREATED_RESOURCES, createdResourceArray);
                    transactionHelper.writeUser(creatorId.toString(), creatorObject);
                }
            } catch (ConnectionException | NotFoundException | AbortException e) {
                e.printStackTrace();
                return -1;
            }
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument. Get the profile object
     * for a user.
     *
     * @param requesterID    Unique identifier for the requester.
     * @param profileOwnerID unique profile owner's identifier.
     * @param result         A HashMap with all data  returned. These data are different user information within the
     *                       profile such as friend count, friend request count, etc.
     * @param insertImage    Identifies if the users have images in the database. If set to true the images for the
     *                       users will be retrieved.
     * @param testMode       If set to true images will be retrieved and stored on the file system. While running
     *                       benchmarks this field should be set to false.
     * @return 0 on success a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function retrieves the user's profile details, friendcount (number of friends for that
     * user) and resourcecount (number of resources inserted on that user's wall). In addition if the requesterID is
     * equal to the profileOwnerID, the pendingcount (number of pending friend requests) needs to be returned as well.
     * <p>
     * If the insertImage is set to true, the image for the profileOwnerID will be rertrieved. The insertImage should be
     * set to true only if the user entity has an image in the database.
     * <p>
     * The friendcount, resourcecount, pendingcount should be put into the results HashMap with the following keys:
     * "friendcount", "resourcecount" and "pendingcount", respectively. Lack of these attributes or not returning the
     * attribute keys in lower case causes BG to raise exceptions. In addition, all other attributes of the profile need
     * to be added to the result hashmap with the attribute names being the keys and the attribute values being the
     * values in the hashmap.
     * <p>
     * If images exist for users, they should be converted to bytearrays and added to the result hashmap.
     */
    @Override
    public synchronized int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
                                        boolean insertImage, boolean testMode) {
        JsonObject jsonObject;
        try {
            jsonObject = transactionHelper.readUser(String.valueOf(profileOwnerID));
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }

        /**
         * Dump data to result.
         */
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            if (!entry.getKey().equals(PENDING_FRIENDS) && !entry.getKey().equals(CONFIRMED_FRIENDS) && !entry.getKey
                    ().equals(RESOURCES) && !entry.getKey().equals(CREATED_RESOURCES) && !entry.getKey().equals(PIC)
                    && !entry.getKey().equals(TPIC)) {
                result.put(entry.getKey(), new StringByteIterator(entry.getValue().getAsString()));
            } else if (insertImage && (entry.getKey().equals(PIC) || entry.getKey().equals(TPIC))) {
                result.put(entry.getKey(), ImageUtils.toObejctByteIterator(entry.getValue()));
            }
        }

        /**
         * Count friends.
         */
        int friendCount = 0;
        if (jsonObject.has(CONFIRMED_FRIENDS)) {
            JsonArray jsonArray = jsonObject.getAsJsonArray(CONFIRMED_FRIENDS);
            friendCount = jsonArray.size();
        }
        result.put(FRIEND_COUNT, new ObjectByteIterator(String.valueOf(friendCount).getBytes()));

        /**
         * Count resources.
         */
        int resourceCount = 0;
        if (jsonObject.has(RESOURCES)) {
            JsonArray jsonArray = jsonObject.getAsJsonArray(RESOURCES);
            resourceCount = jsonArray.size();
        }
        result.put(RESOURCE_COUNT, new ObjectByteIterator(String.valueOf(resourceCount).getBytes()));

        /**
         * Pending friendships.
         */
        if (requesterID == profileOwnerID) {
            int pendingCount = 0;
            if (jsonObject.has(PENDING_FRIENDS)) {
                JsonArray jsonArray = jsonObject.getAsJsonArray(PENDING_FRIENDS);
                pendingCount = jsonArray.size();
            }
            result.put(PENDING_COUNT, new ObjectByteIterator(String.valueOf(pendingCount).getBytes()));
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * gets the list of friends for a member.
     *
     * @param requesterID    The unique identifier of the user who wants to view profile owners friends.
     * @param profileOwnerID The id of the profile for which the friends are listed.
     * @param fields         Contains the attribute names required for each friend. This can be set to null to retrieve
     *                       all the friend information.
     * @param result         A Vector of HashMaps, where each HashMap is a set field/value pairs for one friend.
     * @param insertImage    If set to true the thumbnail images for the friends will be retrieved from the data store.
     * @param testMode       If set to true the thumbnail images of friends will be written to the file system.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function should retrieve the list of friends for the profileOwnerID. The information
     * retrieved per friend depends on the fields specified in the fields set. if fields is set to null all profile
     * information for the friends is retrieved and the result hashmap is populated. The friend's unique id should be
     * inserted with the "userid" key into the result hashmap. The lack of this attribute or the lack of the attribute
     * key in lower case causes BG to raise exceptions. In addition if the insertImage flag is set to true, the
     * thumbnails for each friend's profile should be retrieved and inserted into the result hashmap using the "pic"
     * key.
     */
    @Override
    public synchronized int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
                                        Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean
                                                    testMode) {
        try {
            JsonObject ownerObject = transactionHelper.readUser(String.valueOf(profileOwnerID));

            if (ownerObject.has(CONFIRMED_FRIENDS)) {
                JsonArray friendArray = ownerObject.getAsJsonArray(CONFIRMED_FRIENDS);

                /**
                 * Read all the friends.
                 */
                for (JsonElement element : friendArray) {
                    JsonObject friendObject = transactionHelper.readUser(element.getAsString());
                    HashMap<String, ByteIterator> hashMap = new HashMap<>();

                    if (fields == null) {
                        for (Map.Entry<String, JsonElement> entry : friendObject.entrySet()) {
                            if (insertImage && (entry.getKey().equals(PIC) || entry.getKey().equals(TPIC))) {
                                hashMap.put(entry.getKey(), ImageUtils.toObejctByteIterator(entry.getValue()));
                            } else if (!entry.getValue().isJsonArray()) {
                                hashMap.put(entry.getKey(), new StringByteIterator(entry.getValue().getAsString()));
                            }
                        }
                    } else {
                        for (String field : fields) {
                            if (insertImage && (field.equals(PIC) || field.equals(TPIC))) {
                                hashMap.put(field, ImageUtils.toObejctByteIterator(friendObject.get(field)));
                            } else {
                                hashMap.put(field, new StringByteIterator(friendObject.get(field).getAsString()));
                            }
                        }
                    }
                    result.add(hashMap);
                }
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }

        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * gets the list of pending friend requests for a member. These are the requests that are generated for the
     * profileOwnerID but have not been accepted or rejected.
     *
     * @param profileOwnerID The profile owner's unique identifier.
     * @param results        A vector of hashmaps where every hashmap belongs to one inviter.
     * @param insertImage    If set to true the images for the friends will be retrieved from the data store.
     * @param testMode       If set to true the thumbnail images of friends will be written to the file system.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function should retrieve the list of pending invitations for the profileOwnerID's
     * profile, all profile information for the pending friends is retrieved and the result hashmap is populated. The
     * unique id of the friend generating the request should be added using the "userid" key to the hasmap. The lack of
     * this attribute or the lack of the attribute key in lower case causes BG to raise exceptions. In addition if the
     * insertImage flag is set to true, the thumbnails for each pending friend profile should be retrieved and inserted
     * into the result hashmap.
     */
    @Override
    public synchronized int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean
            insertImage, boolean testMode) {
        try {
            JsonObject jsonObject = transactionHelper.readUser(String.valueOf(profileOwnerID));
            if (jsonObject.has(PENDING_FRIENDS)) {
                JsonArray friendArray = jsonObject.get(PENDING_FRIENDS).getAsJsonArray();
                for (JsonElement element : friendArray) {
                    JsonObject requesterObject = transactionHelper.readUser(element.getAsString());

                    HashMap<String, ByteIterator> hashMap = new HashMap<>();
                    for (Map.Entry<String, JsonElement> entry : requesterObject.entrySet()) {
                        if (insertImage && (entry.getKey().equals(PIC) || entry.getKey().equals(TPIC))) {
                            hashMap.put(entry.getKey(), ImageUtils.toObejctByteIterator(entry.getValue()));
                        } else if (!entry.getValue().isJsonArray()) {
                            hashMap.put(entry.getKey(), new StringByteIterator(entry.getValue().getAsString()));
                        }
                    }
                    results.add(hashMap);
                }
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * Accepts a pending friend request. This action can only be done by the invitee.
     *
     * @param inviterID The unique identifier of the inviter (the user who generates the friend request).
     * @param inviteeID The unique identifier of the invitee (the person who receives the friend request).
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function should accept the friendship request and generate a friendship relationship
     * between inviteeID and inviterID. The friendship relationship is symmetric, so if A is friends with B, B also
     * should be friends with A.
     */
    @Override
    public synchronized int acceptFriend(int inviterID, int inviteeID) {
        try {
            JsonObject inviterObject = transactionHelper.readUser(String.valueOf(inviterID));
            JsonObject inviteeObject = transactionHelper.readUser(String.valueOf(inviteeID));

            /**
             * Inviter.
             */
            JsonArray confirmedFriends;
            if (inviterObject.has(CONFIRMED_FRIENDS)) {
                confirmedFriends = inviterObject.getAsJsonArray(CONFIRMED_FRIENDS);
            } else {
                confirmedFriends = new JsonArray();
            }

            inviterObject.add(CONFIRMED_FRIENDS, confirmedFriends);
            transactionHelper.writeUser(String.valueOf(inviterID), inviterObject);

            /**
             * Invitee.
             */
            if (inviteeObject.has(CONFIRMED_FRIENDS)) {
                confirmedFriends = inviteeObject.getAsJsonArray(CONFIRMED_FRIENDS);
            } else {
                confirmedFriends = new JsonArray();
            }

            confirmedFriends.add(new JsonPrimitive(String.valueOf(inviteeID)));
            inviteeObject.add(CONFIRMED_FRIENDS, confirmedFriends);

            /**
             * Remove from pending friends.
             */
            JsonArray pendingFriends;
            if (inviteeObject.has(PENDING_FRIENDS)) {
                pendingFriends = inviteeObject.getAsJsonArray(PENDING_FRIENDS);
            } else {
                pendingFriends = new JsonArray();
            }

            pendingFriends.remove(new JsonPrimitive(String.valueOf(inviterID)));
            transactionHelper.writeUser(String.valueOf(inviteeID), inviteeObject);
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * Rejects a pending friend request. This action can only be done by the invitee.
     *
     * @param inviterID The unique identifier of the inviter (The person who generates the friend request).
     * @param inviteeID The unique identifier of the invitee (The person who receives the friend request).
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function should remove the friend request generated by the inviterID without creating
     * any friendship relationship between the inviterID and the inviteeID.
     */
    @Override
    public synchronized int rejectFriend(int inviterID, int inviteeID) {
        try {
            JsonObject inviteeObject = transactionHelper.readUser(String.valueOf(inviteeID));

            JsonArray pendingFriends;
            if (inviteeObject.has(PENDING_FRIENDS)) {
                pendingFriends = inviteeObject.getAsJsonArray(PENDING_FRIENDS);
            } else {
                pendingFriends = new JsonArray();
            }

            pendingFriends.remove(new JsonPrimitive(String.valueOf(inviterID)));
            transactionHelper.writeUser(String.valueOf(inviteeID), inviteeObject);
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called both in the benchmarking phase executed with the -t argument and the load phase executed
     * either using -load or the -loadindex argument.
     * <p>
     * Generates a friend request which can be considered as generating a pending friendship.
     *
     * @param inviterID The unique identifier of the inviter (the person who generates the friend request).
     * @param inviteeID The unique identifier of the invitee (the person who receives the friend request).
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function should generate a friend request invitation which is extended by the inviterID
     * to the inviteeID.
     */
    @Override
    public synchronized int inviteFriend(int inviterID, int inviteeID) {
        try {
            JsonObject inviteeObject = transactionHelper.readUser(String.valueOf(inviteeID));

            JsonArray pendingFriends;
            if (inviteeObject.has(PENDING_FRIENDS)) {
                pendingFriends = inviteeObject.getAsJsonArray(PENDING_FRIENDS);
            } else {
                pendingFriends = new JsonArray();
            }

            pendingFriends.add(new JsonPrimitive(String.valueOf(inviterID)));
            inviteeObject.add(PENDING_FRIENDS, pendingFriends);
            transactionHelper.writeUser(String.valueOf(inviteeID), inviteeObject);
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * gets the top k resources posted on a member's wall. These can be created by the user on her own wall or by other
     * users on this user's wall.
     *
     * @param requesterID    The unique identifier of the user who wants to view profile owners resources.
     * @param profileOwnerID The profile owner's unique identifier.
     * @param k              The number of resources requested.
     * @param result         A vector of all the resource entities, every resource entity is a hashmap containing
     *                       resource attributes. This hashmap should have the resource id identified by "rid" and the
     *                       wall user id identified by "walluserid". The lack of these attributes or not having these
     *                       keys in lower case may cause BG to raise exceptions.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     */
    @Override
    public synchronized int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String,
            ByteIterator>> result) {
        try {
            JsonObject userObject = transactionHelper.readUser(String.valueOf(profileOwnerID));
            if (userObject.has(RESOURCES)) {
                JsonArray resourceArray = userObject.getAsJsonArray(RESOURCES);
                for (int i = 0; i < resourceArray.size() && i < k; i++) {
                    JsonObject resourceObject = transactionHelper.readResource(resourceArray.get(i).getAsString());

                    HashMap<String, ByteIterator> values = new HashMap<>();
                    for (Map.Entry<String, JsonElement> entry : resourceObject.entrySet()) {
                        if (entry.getValue().isJsonPrimitive()) {
                            values.put(entry.getKey(), new StringByteIterator(entry.getValue().getAsString()));
                        }
                    }
                    result.add(values);
                }
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * gets the  resources  created by a user, the created resources may be posted either on the user's wall or on
     * another user's wall.
     *
     * @param creatorID The unique identifier of the user who created the resources.
     * @param result    A vector of all the resource entities, every entity is a hashmap containing the attributes of a
     *                  resource and their values without considering the comments on the resource. Every resource
     *                  should have a unique identifier which will be copied into the hashmap using the "rid" key and a
     *                  unique creator which will be copied into the "creatorid" key. Lack of this key in lower case may
     *                  cause BG to raise exceptions.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     */
    @Override
    public synchronized int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
        try {
            JsonObject creatorObject = transactionHelper.readUser(String.valueOf(creatorID));

            if (creatorObject.has(CREATED_RESOURCES)) {
                JsonArray resourceArray = creatorObject.getAsJsonArray(CREATED_RESOURCES);

                for (JsonElement element : resourceArray) {
                    JsonObject resourceObject = transactionHelper.readResource(element.getAsString());

                    HashMap<String, ByteIterator> hashMap = new HashMap<>();
                    for (Map.Entry<String, JsonElement> entry : resourceObject.entrySet()) {
                        hashMap.put(entry.getKey(), new StringByteIterator(entry.getValue().getAsString()));
                    }

                    result.add(hashMap);
                }
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * gets the comments for a resource and all the comment's details.
     *
     * @param requesterID    The unique identifier of the user who wants to view the comments posted on the
     *                       profileOwnerID's resource.
     * @param profileOwnerID The profile owner's unique identifier (owner of the resource).
     * @param resourceID     The resource's unique identifier.
     * @param result         A vector of all the comment entities for a specific resource, each comment and its details
     *                       are specified as a hashmap.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written for this function, gets the resourceid of a resource and returns all the comments posted on that
     * resource and their details. This information should be put into the results Vector.
     */
    @Override
    public synchronized int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
                                                  Vector<HashMap<String, ByteIterator>> result) {
        try {
            JsonObject manipulationsObject = transactionHelper.readManipulations(String.valueOf(resourceID));

            for (Map.Entry<String, JsonElement> manipulationEntry : manipulationsObject.entrySet()) {
                JsonObject manipulationObject = manipulationEntry.getValue().getAsJsonObject();

                HashMap<String, ByteIterator> hashMap = new HashMap<>();
                for (Map.Entry<String, JsonElement> attrEntry : manipulationObject.entrySet()) {
                    hashMap.put(attrEntry.getKey(), new StringByteIterator(attrEntry.getValue().getAsString()));
                }

                result.add(hashMap);
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase executed with the -t argument.
     * <p>
     * posts/creates a comment on a specific resource. Every comment created is inserted into the manipulation entity
     * set.
     *
     * @param commentCreatorID  The unique identifier of the user who is creating the comment.
     * @param resourceCreatorID The resource creator's unique identifier (owner of the resource).
     * @param resourceID        The resource's unique identifier.
     * @param values            The values for the comment which contains the unique identifier of the comment
     *                          identified by "mid".
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes. The code written in this function, creates a comment on the resource identified with resourceID and
     * created by profileOwnerID.
     */
    @Override
    public synchronized int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
                                                  HashMap<String, ByteIterator> values) {
        JsonObject manipulationObject = new JsonObject();
        String manipulationId = "";

        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            if (entry.getKey().equals("mid")) {
                manipulationId = entry.getValue().toString();
            }
            manipulationObject.add(entry.getKey(), new JsonPrimitive(entry.getValue().toString()));
        }

        try {
            transactionHelper.writeManipulation(String.valueOf(resourceID), manipulationId, manipulationObject);
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase executed with the -t argument. deletes a specific comment on a
     * specific resource.
     *
     * @param resourceCreatorID The resource creator's unique identifier (owner of the resource).
     * @param resourceID        The resource's unique identifier.
     * @param manipulationID    The unique identifier of the comment to be deleted.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes. The code written in this function, deletes a comment identified by manipulationID on the resource
     * identified with resourceID and created by the resourceCreatorID.
     */
    @Override
    public synchronized int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
        try {
            transactionHelper.deleteManipulation(String.valueOf(resourceID), String.valueOf(manipulationID));
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * Thaws a friendship.
     *
     * @param friendid1 The unique identifier of the person who wants to remove a friend.
     * @param friendid2 The unique identifier of the friend to be removed.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written in this function terminates the friendship relationship between friendid1 and friendid2. The
     * friendship relationship should be removed for both friendid1 and friendid2. The friendship/thaw friendship
     * relationship is symmetric so if A is not friends with B, B also will not be friends with A.
     */
    @Override
    public synchronized int thawFriendship(int friendid1, int friendid2) {
        try {
            final String strFriendId1 = String.valueOf(friendid1);
            final String strFriendId2 = String.valueOf(friendid2);

            JsonObject friendObject1 = transactionHelper.readUser(strFriendId1);
            JsonObject friendObject2 = transactionHelper.readUser(strFriendId2);

            if (friendObject1.has(CONFIRMED_FRIENDS) && friendObject2.has(CONFIRMED_FRIENDS)) {
                JsonArray friendArray1 = friendObject1.getAsJsonArray(CONFIRMED_FRIENDS);
                JsonArray friendArray2 = friendObject2.getAsJsonArray(CONFIRMED_FRIENDS);

                friendArray1.remove(new JsonPrimitive(strFriendId2));
                friendArray2.remove(new JsonPrimitive(strFriendId1));

                friendObject1.add(CONFIRMED_FRIENDS, friendArray1);
                friendObject2.add(CONFIRMED_FRIENDS, friendArray2);

                transactionHelper.writeUser(strFriendId1, friendObject1);
                transactionHelper.writeUser(strFriendId2, friendObject2);
            }
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the load phase which is executed using the -load or -loadindex argument
     * <p>
     * returns DB's initial statistics. These statistics include number of users, average number of friends per user,
     * average number of pending friend requests per user, and number of resources per user. The initial statistics are
     * queried and inserted into a hashmap. This hashmap should contain the following attributes: "usercount",
     * "resourcesperuser", "avgfriendsperuser", "avgpendingperuser" The lack of these attributes or the lack of the keys
     * in lower case causes BG to raise exceptions.
     *
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     */
    @Override
    public synchronized HashMap<String, String> getInitialStats() {
        int userCount = 0;
        while (true) {
            try {
                JsonObject userObject = transactionHelper.readUser(String.valueOf(userCount));
                userCount++;
            } catch (ConnectionException | NotFoundException e) {
                break;
            }
        }
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put(USER_COUNT, String.valueOf(userCount));

        try {
                JsonObject userObject = transactionHelper.readUser("0");

                int resourceCount = 0;
                int friendCount = 0;
                int pendingCount = 0;

                if (userObject.has(RESOURCES)) {
                    resourceCount += userObject.getAsJsonArray(RESOURCES).size();
                }
                if (userObject.has(CONFIRMED_FRIENDS)) {
                    friendCount += userObject.getAsJsonArray(CONFIRMED_FRIENDS).size();
                }
                if (userObject.has(PENDING_FRIENDS)) {
                    pendingCount += userObject.getAsJsonArray(PENDING_FRIENDS).size();
                }

                hashMap.put(RESOURCES_PER_USER, String.valueOf(resourceCount));
                hashMap.put(AVG_FRIENDS_PER_USER, String.valueOf(friendCount));
                hashMap.put(AVG_PENDING_PER_USER, String.valueOf(pendingCount));
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return null;
        }
        return hashMap;
    }

    /**
     * This function is called in the load phase which is executed using the -load or -loadindex argument Creates a
     * confirmed friendship between friendid1 and friendid2.
     *
     * @param friendid1 The unique identifier of the first member.
     * @param friendid2 The unique identifier of the second member.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     * <p>
     * The code written in this function generates a friendship relationship between friendid1 and friendid2 The
     * friendship relationship is symmetric, so if A is friends with B, B is also friends with A.
     */
    @Override
    public synchronized int CreateFriendship(int friendid1, int friendid2) {
        try {
            final String strFriendId1 = String.valueOf(friendid1);
            final String strFriendId2 = String.valueOf(friendid2);

            JsonObject friendObject1 = transactionHelper.readUser(strFriendId1);
            JsonObject friendObject2 = transactionHelper.readUser(strFriendId2);

            if (!friendObject1.has(CONFIRMED_FRIENDS)) {
                friendObject1.add(CONFIRMED_FRIENDS, new JsonArray());
            }
            if (!friendObject2.has(CONFIRMED_FRIENDS)) {
                friendObject2.add(CONFIRMED_FRIENDS, new JsonArray());
            }

            JsonArray friendArray1 = friendObject1.getAsJsonArray(CONFIRMED_FRIENDS);
            JsonArray friendArray2 = friendObject2.getAsJsonArray(CONFIRMED_FRIENDS);

            friendArray1.add(new JsonPrimitive(strFriendId2));
            friendArray2.add(new JsonPrimitive(strFriendId1));

            //friendObject1.add(CONFIRMED_FRIENDS, friendArray1);
            //friendObject2.add(CONFIRMED_FRIENDS, friendArray2);

            transactionHelper.writeUser(strFriendId1, friendObject1);
            transactionHelper.writeUser(strFriendId2, friendObject2);
        } catch (ConnectionException | NotFoundException | AbortException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the schema creation phase which is executed with the -schema argument. Creates the
     * data store schema which will then be populated in the load phase. Depending on the type of datastore, the code
     * for creating index structures may also be provided within this function call.
     *
     * @param props The properties of BG.
     *              <p>
     *              BG dictates a fixed conceptual schema. This schema consists of three entity sets: users, resources,
     *              manipulations The attributes for each entity set are as follows; 1) users (userid, username, pw,
     *              fname, lname, gender, dob, jdate, ldate, address, email, tel, tpic, pic) tpic and pic are available
     *              if there are images inserted for users. 2) resources (rid, creatorid,  walluserid, type, body, doc)
     *              3) manipulations (mid, creatorid, rid, modifierid, timestamp, type, content)
     */
    @Override
    public synchronized void createSchema(Properties props) {
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * Queries the inviterid's of pending friendship requests for a member specified by memberID.
     *
     * @param memberID   The unique identifier of the user.
     * @param pendingIds Is a vector of all the member ids that have created a friendship invitation for memberID.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     */
    @Override
    public synchronized int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
        try {
            JsonObject jsonObject = transactionHelper.readUser(String.valueOf(memberID));
            if (jsonObject.has(PENDING_FRIENDS)) {
                JsonArray jsonArray = jsonObject.getAsJsonArray(PENDING_FRIENDS);
                for (JsonElement jsonElement : jsonArray) {
                    int friendId = Integer.parseInt(jsonElement.getAsString());
                    pendingIds.add(friendId);
                }
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    /**
     * This function is called in the benchmarking phase which is executed with the -t argument.
     * <p>
     * Queries the friendids of confirmed friendships for a member specified by memberID.
     *
     * @param memberID     The unique identifier of the user.
     * @param confirmedIds Is a vector of all the member ids that have a friendship relationship with memberID.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error
     * codes.
     */
    @Override
    public synchronized int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
        try {
            JsonObject jsonObject = transactionHelper.readUser(String.valueOf(memberID));
            if (jsonObject.has(CONFIRMED_FRIENDS)) {
                JsonArray jsonArray = jsonObject.getAsJsonArray(CONFIRMED_FRIENDS);
                for (JsonElement jsonElement : jsonArray) {
                    int friendId = Integer.parseInt(jsonElement.getAsString());
                    confirmedIds.add(friendId);
                }
            }
        } catch (ConnectionException | NotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
}
