package org.springframework.batch.core.repository.support.incrementer;

import static org.junit.Assert.*;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.support.incrementer.MongoMaxValueIncrementer.UniqueIdentifierCounter;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Riccardo Cardin
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class MongoMaxValueIncrementerIntegrationTests {
  
  // TODO find the best way to externalize and manage image versions
  private static final DockerImageName MONGODB_IMAGE = DockerImageName.parse("mongo:5.0.2");
  public static final String DATABASE_NAME = "test";
  public static final String SEQUENCES_COLLECTION = "sequences";
  
  @ClassRule
  public static MongoDBContainer mongodb = new MongoDBContainer(MONGODB_IMAGE);
  
  private MongoMaxValueIncrementer incrementer;

  @Before
  public void setUp() throws Exception {
    final String mongodbUrl = mongodb.getReplicaSetUrl();
    final MongoClient mongoClient = MongoClients.create(mongodbUrl);
  
    initializeDatabase(mongoClient);
  
    incrementer = new MongoMaxValueIncrementer(
        mongoClient,
        DATABASE_NAME,
        SEQUENCES_COLLECTION
    );
  }
  
  private void initializeDatabase(MongoClient mongoClient) {
    final MongoTemplate mongo = new MongoTemplate(mongoClient, DATABASE_NAME);
    mongo.dropCollection(SEQUENCES_COLLECTION);
    mongo.insert(
        new UniqueIdentifierCounter(
            "UNIQUE_COUNT_DOCUMENT_IDENTIFIER",
            0L,
            ""
        ),
        SEQUENCES_COLLECTION
    );
  }
  
  @Test
  public void nextIntValue() {
  
  }
  
  @Test
  public void nextStringValue() {
  }
  
  @Test
  public void testNextLongValue() {
  
//    final ExecutorService executorService = Executors.newFixedThreadPool(10);
//    final List<CompletableFuture<Long>> futures = Stream.generate(
//            () -> CompletableFuture.supplyAsync(incrementer::nextLongValue, executorService))
//        .limit(1000)
//        .collect(Collectors.toList());
//
//    CompletableFuture.allOf();
  
    final long firstValueOfTheSequence = incrementer.nextLongValue();
    assertEquals(0L, firstValueOfTheSequence);
    final long secondValueOfTheSequence = incrementer.nextLongValue();
    assertEquals(1L, secondValueOfTheSequence);
  }
  
  @Configuration
  static class TestConfiguration {
  
  }
}