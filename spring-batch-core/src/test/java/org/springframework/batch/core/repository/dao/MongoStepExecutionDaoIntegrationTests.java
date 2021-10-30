package org.springframework.batch.core.repository.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.Collections;
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
    shouldFailIfNoStepExecutionIsProvided();
    shouldFailIfStepNameIsEmpty();
    shouldFailIfStartTimeIsEmpty();
    shouldFailIfStatusIsEmpty();
    shouldFailIfIdIsNotEmpty();
    shouldFailIfVersionIsNotEmpty();
    shouldInsertValidStepExecution();
  }
  
  private void shouldFailIfNoStepExecutionIsProvided() {
    assertThrows(
        "stepExecution is required",
        IllegalArgumentException.class,
        () -> dao.saveStepExecution(null)
    );
  }
  
  private void shouldFailIfStepNameIsEmpty() {
    assertThrows(
        "StepExecution step name cannot be null.",
        IllegalArgumentException.class,
        () -> dao.saveStepExecution(Fixture.STEP_EXECUTION_WITH_EMPTY_STEP_NAME)
    );
  }
  
  
  private void shouldFailIfStartTimeIsEmpty() {
    assertThrows(
        "StepExecution start time cannot be null.",
        IllegalArgumentException.class,
        () -> dao.saveStepExecution(Fixture.STEP_EXECUTION_WITH_EMPTY_START_TIME)
    );
  }
  
  private void shouldFailIfStatusIsEmpty() {
    assertThrows(
        "StepExecution status cannot be null.",
        IllegalArgumentException.class,
        () -> dao.saveStepExecution(Fixture.STEP_EXECUTION_WITH_EMPTY_STATUS)
    );
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
  
  private void shouldInsertValidStepExecution() {
    final StepExecution stepExecution = Fixture.makeStepExecutionWithEmptyId();
    dao.saveStepExecution(stepExecution);
    assertNotNull(stepExecution.getId());
  }

  @Test
  public void testSaveStepExecutions() {
    shouldFailIfNoStepExecutionsAreProvided();
    shouldInsertValidStepExecutions();
  }
  
  private void shouldFailIfNoStepExecutionsAreProvided() {
    assertThrows(
        "Attempt to save a null collection of step executions",
        IllegalArgumentException.class,
        () -> dao.saveStepExecutions(null)
    );
  }
  
  private void shouldInsertValidStepExecutions() {
    final StepExecution stepExecution = Fixture.makeStepExecutionWithEmptyId();
    dao.saveStepExecutions(Collections.singletonList(stepExecution));
    assertNotNull(stepExecution.getId());
  }
  
  @Configuration
  static class TestConfiguration {}
  
  static abstract class Fixture {
    
    static final StepExecution STEP_EXECUTION_WITH_EMPTY_STEP_NAME =
        makeStepExecutionWithoutStepName();
    
    static final StepExecution STEP_EXECUTION_WITH_NOT_EMPTY_ID = new StepExecution(
        "stepName",
        mock(JobExecution.class),
        42L
    );
    
    static final StepExecution STEP_EXECUTION_WITH_NOT_EMPTY_VERSION =
        makeStepExecutionWithVersion();
  
    static final StepExecution STEP_EXECUTION_WITH_EMPTY_START_TIME =
        makeStepExecutionWithEmptyStartTime();
    
    static final StepExecution STEP_EXECUTION_WITH_EMPTY_STATUS =
        makeStepExecutionWithEmptyStatus();
  
    private static StepExecution makeStepExecutionWithEmptyStartTime() {
      final StepExecution stepExecution = mock(StepExecution.class);
      doReturn(null).when(stepExecution).getStartTime();
      return stepExecution;
    }
  
    private static StepExecution makeStepExecutionWithoutStepName() {
      final StepExecution stepExecution = mock(StepExecution.class);
      doReturn(null).when(stepExecution).getStepName();
      return stepExecution;
    }
  
    private static StepExecution makeStepExecutionWithVersion() {
      final StepExecution stepExecution = new StepExecution(
          "stepName",
          mock(JobExecution.class)
      );
      stepExecution.setVersion(1);
      return stepExecution;
    }
  
    private static StepExecution makeStepExecutionWithEmptyStatus() {
      final StepExecution stepExecution = mock(StepExecution.class);
      doReturn(null).when(stepExecution).getStatus();
      return stepExecution;
    }
  
    static StepExecution makeStepExecutionWithEmptyId() {
      return new StepExecution(
          "stepName",
          mock(JobExecution.class)
      );
    }
  }
}