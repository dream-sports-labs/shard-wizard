package com.dream11.shardwizard.example.runs.client.postgres;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.example.runs.client.OrderDaoFactoryBaseIntegrationTest;
import com.dream11.shardwizard.model.ShardDetails;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;

@Slf4j
public class OrderDaoFactoryIntegrationTest extends OrderDaoFactoryBaseIntegrationTest {

  static {
    CONFIGURED_ROUND = 1012;
  }

  @BeforeAll
  public static void setUp() throws Exception {
    log.info("Starting PostgreSQL integration test setup");
    setupBaseWithDatabaseType(DatabaseType.POSTGRES);
    log.info("PostgreSQL integration test setup completed");
  }

  // PostgreSQL-specific implementation methods
  @Override
  protected int[] getTestRoundsAndUserIds() {
    return new int[] {4446, 11125, 60009002};
  }

  @Override
  protected int getTestRoundForShardDetails() {
    return 1009;
  }

  @Override
  protected List<ShardDetails> getExpectedShardDetails(int roundId) {
    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    if (roundId == 1009) {
      expectedShardDetails.add(createPostgresShard(3, 5435));
      expectedShardDetails.add(createPostgresShard(4, 5436));
    } else if (roundId == CONFIGURED_ROUND) {
      expectedShardDetails.add(createPostgresShard(1, 5433));
      expectedShardDetails.add(createPostgresShard(2, 5434));
    }
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
    log.info("Creating first order for userId: {}, round: {}", userId1, roundId);

    saveOrder(user1Order, roundId, userId1)
        .flatMap(ord1 -> verifyFirstOrder(ord1, userId1))
        .flatMap(order1DTO -> createAndVerifySecondOrder(roundId, userId2))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in order creation test", e, latch, error));
  }
}
