package org.springframework.batch.core.repository.support.incrementer;

import static org.junit.Assert.*;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
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
public class MongoMaxValueIncrementerIntegrationTests {
  
  // TODO find the best way to externalize and manage image versions
  private static final DockerImageName MONGODB_IMAGE = DockerImageName.parse("mongo:5.0.2");
  
  @ClassRule
  public static MongoDBContainer mongodb = new MongoDBContainer(MONGODB_IMAGE);
  
  @Test
  public void nextIntValue() {
  }
  
  @Test
  public void nextStringValue() {
  }
  
  @Test
  public void nextLongValue() {
  }
  
  @Configuration
  static class TestConfiguration {
  
  }
}