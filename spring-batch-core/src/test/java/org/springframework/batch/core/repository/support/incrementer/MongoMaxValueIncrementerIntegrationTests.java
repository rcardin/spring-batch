package org.springframework.batch.core.repository.support.incrementer;

import static org.junit.Assert.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Riccardo Cardin
 * @author Giuseppe Pavan
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class MongoMaxValueIncrementerIntegrationTests {
  
  // TODO find the best way to externalize and manage image versions
  private static final DockerImageName MONGODB_IMAGE = DockerImageName.parse("mongo:5.0.2");

  private static final String DATABASE_NAME = "MongoMaxValueIncrementerIntegrationTests";
  public static final int TOTAL_INCREMENTS = 1000;
  public static final int CONCURRENT_INCREMENTS = 10;
  
  @ClassRule public static MongoDBContainer mongodb = new MongoDBContainer(MONGODB_IMAGE);

  private MongoClient mongoClient;

  @Before
  public void setUp() throws Exception {
    final String mongodbUrl = mongodb.getReplicaSetUrl();
    mongoClient = MongoClients.create(mongodbUrl);
  }

  @Test
  public void testNextIntValue() {
    final MongoMaxValueIncrementer incrementer =
        new MongoMaxValueIncrementer(mongoClient, DATABASE_NAME, "nextIntValue");

    final int firstValueOfTheSequence = incrementer.nextIntValue();
    final int secondValueOfTheSequence = incrementer.nextIntValue();

    assertEquals(1, firstValueOfTheSequence);
    assertEquals(2, secondValueOfTheSequence);
  }

  @Test
  public void testNextStringValue() {
    final MongoMaxValueIncrementer incrementer =
        new MongoMaxValueIncrementer(mongoClient, DATABASE_NAME, "nextStringValue");

    final String firstValueOfTheSequence = incrementer.nextStringValue();
    final String secondValueOfTheSequence = incrementer.nextStringValue();

    assertEquals("1", firstValueOfTheSequence);
    assertEquals("2", secondValueOfTheSequence);
  }

  @Test
  public void testNextLongValue() {
    final MongoMaxValueIncrementer incrementer =
        new MongoMaxValueIncrementer(mongoClient, DATABASE_NAME, "testNextLongValue");

    final long firstValueOfTheSequence = incrementer.nextLongValue();
    final long secondValueOfTheSequence = incrementer.nextLongValue();

    assertEquals(1L, firstValueOfTheSequence);
    assertEquals(2L, secondValueOfTheSequence);
  }
  
  @Test
  public void testNextLongValueConcurrentAccess() {
    final MongoMaxValueIncrementer incrementer =
        new MongoMaxValueIncrementer(
            mongoClient, DATABASE_NAME, "testNextLongValueConcurrentAccess");

    final List<CompletableFuture<Long>> incrementTasks = runConcurrentIncrements(incrementer);

    CompletableFuture.allOf(incrementTasks.toArray(new CompletableFuture[0])).join();

    final OptionalLong actualSequenceValue =
        incrementTasks.stream()
            .map(this::extractValue)
            .mapToLong(Long::longValue)
            .max();

    assertEquals(OptionalLong.of(1000), actualSequenceValue);
  }
  
  private Long extractValue(CompletableFuture<Long> f) {
    try {
      return f.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
  
  private List<CompletableFuture<Long>> runConcurrentIncrements(
      MongoMaxValueIncrementer incrementer) {
    final ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_INCREMENTS);
    return Stream.generate(
            () -> CompletableFuture.supplyAsync(incrementer::nextLongValue, executorService))
        .limit(TOTAL_INCREMENTS)
        .collect(Collectors.toList());
  }

  @Configuration
  static class TestConfiguration {}
}
