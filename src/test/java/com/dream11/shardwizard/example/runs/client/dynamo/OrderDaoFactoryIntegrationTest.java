package com.dream11.shardwizard.example.runs.client.dynamo;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.example.runs.client.OrderDaoFactoryBaseIntegrationTest;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderDaoFactoryIntegrationTest extends OrderDaoFactoryBaseIntegrationTest {

  static {
    CONFIGURED_ROUND = 1013;
  }

  // DynamoDB-specific implementation methods
  @Override
  protected int[] getTestRoundsAndUserIds() {
    return new int[] {CONFIGURED_ROUND, 11125, 60009002};
  }

  @Override
  protected int getTestRoundForShardDetails() {
    return CONFIGURED_ROUND;
  }

  @Override
  protected List<ShardDetails> getExpectedShardDetails(int roundId) {
    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createPostgresShard(1, 5433));
    expectedShardDetails.add(createDynamoShard(6, 9111));
    return expectedShardDetails;
  }

  @Override
  protected void runOrderCreationAndVerificationTest(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error) {
    OrderDto user1Order = createSampleOrder(userId1, roundId);
    OrderDto user2Order = createSampleOrder(userId2, roundId);

    Single.zip(
            saveOrder(user1Order, roundId, userId1),
            saveOrder(user2Order, roundId, userId2),
            (response1, response2) -> {
              assertNotNull(response1, "Order 1 creation response should not be null");
              assertNotNull(response2, "Order 2 creation response should not be null");
              return new CreateOrderResponseDTO[] {response1, response2};
            })
        .flatMap(responses -> verifyOrders(roundId, userId1, userId2, responses))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in test", e, latch, error));
  }
}
