package org.springframework.batch.core.repository.dao;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Riccardo Cardin
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class MongoStepExecutionDaoIntegrationTests {
  
  @Test
  public void saveStepExecution() {
  }
  
  @Configuration
  static class TestConfiguration {}
}