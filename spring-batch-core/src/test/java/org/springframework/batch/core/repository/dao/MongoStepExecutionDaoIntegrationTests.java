package org.springframework.batch.core.repository.dao;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.support.incrementer.MongoMaxValueIncrementer;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Riccardo Cardin
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class MongoStepExecutionDaoIntegrationTests {
  
  // TODO find the best way to externalize and manage image versions
  private static final DockerImageName MONGODB_IMAGE = DockerImageName.parse("mongo:5.0.2");
  
  private static final String DATABASE_NAME = "MongoStepExecutionDaoIntegrationTests";
  
  @ClassRule
  public static MongoDBContainer mongodb = new MongoDBContainer(MONGODB_IMAGE);
  
  private MongoStepExecutionDao dao;

  @Before
  public void setUp() throws Exception {
    final String mongodbUrl = mongodb.getReplicaSetUrl();
    final MongoClient mongoClient = MongoClients.create(mongodbUrl);
    final MongoMaxValueIncrementer incrementer =
        new MongoMaxValueIncrementer(mongoClient, DATABASE_NAME, "STEP_EXECUTION_SEQ");
    dao = new MongoStepExecutionDao(mongoClient, DATABASE_NAME);
    dao.setStepExecutionIncrementer(incrementer);
  }

  @Test
  public void testSaveStepExecution() {
    shouldFailIfIdIsNotEmpty();
    shouldFailIfVersionIsNotEmpty();
  }
  
  private void shouldFailIfIdIsNotEmpty() {
    assertThrows(
        "to-be-saved (not updated) StepExecution can't already have an id assigned",
        IllegalArgumentException.class,
        () -> dao.saveStepExecution(Fixture.STEP_EXECUTION_WITH_NOT_EMPTY_ID)
    );
  }
  
  private void shouldFailIfVersionIsNotEmpty() {
    assertThrows(
        "to-be-saved (not updated) StepExecution can't already have a version assigned",
        IllegalArgumentException.class,
        () -> dao.saveStepExecution(Fixture.STEP_EXECUTION_WITH_NOT_EMPTY_VERSION)
    );
  }
  
  @Configuration
  static class TestConfiguration {}
  
  static abstract class Fixture {
    
    static final StepExecution STEP_EXECUTION_WITH_NOT_EMPTY_ID = new StepExecution(
        "stepName",
        mock(JobExecution.class),
        42L
    );
    
    static final StepExecution STEP_EXECUTION_WITH_NOT_EMPTY_VERSION = makeStepExecutionWithVersion();
  
    private static StepExecution makeStepExecutionWithVersion() {
      final StepExecution stepExecution = new StepExecution(
          "stepName",
          mock(JobExecution.class)
      );
      stepExecution.setVersion(1);
      return stepExecution;
    }
  }
}