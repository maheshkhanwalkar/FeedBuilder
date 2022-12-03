package edu.columbia.e6156.feed;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Handle upstream post creation event signals and use it to build users'
 * feeds with new posts.
 */
public final class FeedBuilder implements RequestHandler<SNSEvent, Void> {
    // TODO refactor this into several files -- since everything is implemented in this one request
    //  handler class
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDbClient ddbClient = DynamoDbClient.create();

    private static final String RELATION_TRACKER_TABLE = "RelationTracker";
    private static final String FEED_TABLE = "PostFeed";

    private static final String EVENT_TYPE_KEY = "eventType";
    private static final String UNKNOWN_TYPE = "UNKNOWN", CREATED_TYPE = "CREATED";

    private static final String USER_ID = "userId";
    private static final String POST_ID = "postId";

    @Override
    public Void handleRequest(SNSEvent input, Context context) {
        List<SNSEvent.SNSRecord> records = input.getRecords();
        records.forEach(this::handleRecord);

        return null;
    }

    @SuppressWarnings("unchecked")
    private void handleRecord(final SNSEvent.SNSRecord record) {
        String message = record.getSNS().getMessage();

        try {
            Map<String, String> map = objectMapper.readValue(message, Map.class);

            // Ignore other types of events
            if(!map.getOrDefault(EVENT_TYPE_KEY, UNKNOWN_TYPE).equals(CREATED_TYPE)) {
                return;
            }

            String userId = map.get(USER_ID);
            String postId = map.get(POST_ID);

            // Invalid create event -- ignore
            if(userId == null || postId == null) {
                return;
            }

            List<String> followers = getFollowers(userId);
            followers.forEach(follower -> addToFeed(postId, follower));

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private List<String> getFollowers(final String celebrity) {
        QueryRequest request = QueryRequest.builder()
                .tableName(RELATION_TRACKER_TABLE)
                .keyConditionExpression("celebrity = :celeb")
                .expressionAttributeValues(Map.of(":celeb", AttributeValue.fromS(celebrity))).build();

        QueryResponse response = ddbClient.query(request);
        List<Map<String, AttributeValue>> items = response.items();

        return items.stream().map(itemMap -> itemMap.get("follower").s()).collect(toList());
    }

    // FIXME: probably want to do this as a batch put, rather than having multiple puts, to reduce
    //  number of total API calls
    private void addToFeed(String postId, String follower) {
        Map<String, AttributeValue> item = new HashMap<>();

        item.put("userId", AttributeValue.fromS(follower));
        item.put("postId", AttributeValue.fromS(postId));
        item.put("timestamp", AttributeValue.fromN(Long.toString(System.currentTimeMillis())));

        PutItemRequest request = PutItemRequest.builder()
                .tableName(FEED_TABLE)
                .item(item).build();

        ddbClient.putItem(request);
    }
}
