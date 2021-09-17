package org.springframework.batch.core.repository.dao;

import com.mongodb.client.MongoClient;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.support.incrementer.MongoMaxValueIncrementer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.util.Assert;

public class MongoStepExecutionDao implements StepExecutionDao, InitializingBean {

  private static final Log logger = LogFactory.getLog(MongoStepExecutionDao.class);

  private static final int DEFAULT_EXIT_MESSAGE_LENGTH = 2500;

  private static final String COLLECTION_NAME = "BATCH_STEP_EXECUTION";

  private final MongoOperations mongoOperations;
  private MongoMaxValueIncrementer stepExecutionIncrementer;

  public MongoStepExecutionDao(MongoClient mongoClient, String databaseName) {
    this.mongoOperations = new MongoTemplate(mongoClient, databaseName);
  }

  public void setStepExecutionIncrementer(MongoMaxValueIncrementer stepExecutionIncrementer) {
    this.stepExecutionIncrementer = stepExecutionIncrementer;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    Assert.notNull(stepExecutionIncrementer, "StepExecutionIncrementer cannot be null.");
  }

  @Override
  public void saveStepExecution(StepExecution stepExecution) {
    Assert.isNull(
        stepExecution.getId(),
        "to-be-saved (not updated) StepExecution can't already have an id assigned");
    Assert.isNull(
        stepExecution.getVersion(),
        "to-be-saved (not updated) StepExecution can't already have a version assigned");
    validateStepExecution(stepExecution);
    stepExecution.setId(stepExecutionIncrementer.nextLongValue());
    stepExecution.incrementVersion(); // Should be 0
    final MongoStepExecution mongoStepExecution = MongoStepExecution.makeOf(stepExecution);

    mongoOperations.insert(mongoStepExecution, COLLECTION_NAME);
  }

  /**
   * Validate StepExecution. At a minimum, JobId, StartTime, and Status cannot be null. EndTime can
   * be null for an unfinished job.
   *
   * @throws IllegalArgumentException
   */
  private void validateStepExecution(StepExecution stepExecution) {
    Assert.notNull(stepExecution, "stepExecution is required");
    Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
    Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
    Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
  }

  /**
   * Truncate the exit description if the length exceeds {@link #DEFAULT_EXIT_MESSAGE_LENGTH}.
   *
   * @param description the string to truncate
   * @return truncated description
   */
  private static String truncateExitDescription(String description) {
    if (description != null && description.length() > DEFAULT_EXIT_MESSAGE_LENGTH) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Truncating long message before update of StepExecution, original message is: "
                + description);
      }
      return description.substring(0, DEFAULT_EXIT_MESSAGE_LENGTH);
    } else {
      return description;
    }
  }

  @Override
  public void saveStepExecutions(Collection<StepExecution> stepExecutions) {}

  @Override
  public void updateStepExecution(StepExecution stepExecution) {}

  @Override
  public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
    return null;
  }

  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    return null;
  }

  @Override
  public void addStepExecutions(JobExecution jobExecution) {}

  @Override
  public int countStepExecutions(JobInstance jobInstance, String stepName) {
    return 0;
  }

  /** Mongo representation of the object (aka, persistent model) */
  @Document
  static class MongoStepExecution {
    @Id private long stepExecutionId;
    private int version;
    private String stepName;
    private long jobExecutionId;
    private Date starTime;
    private Date endTime;
    private String status;
    private int commitCount;
    private int readCount;
    private int filterCount;
    private int writeCount;
    private String exitCode;
    private String exitMessage;
    private int readSkipCount;
    private int writeSkipCount;
    private int processSkipCount;
    private int rollbackCount;
    private Date lastUpdated;

    public long getStepExecutionId() {
      return stepExecutionId;
    }

    public void setStepExecutionId(long stepExecutionId) {
      this.stepExecutionId = stepExecutionId;
    }

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    public String getStepName() {
      return stepName;
    }

    public void setStepName(String stepName) {
      this.stepName = stepName;
    }

    public long getJobExecutionId() {
      return jobExecutionId;
    }

    public void setJobExecutionId(long jobExecutionId) {
      this.jobExecutionId = jobExecutionId;
    }

    public Date getStarTime() {
      return starTime;
    }

    public void setStarTime(Date starTime) {
      this.starTime = starTime;
    }

    public Date getEndTime() {
      return endTime;
    }

    public void setEndTime(Date endTime) {
      this.endTime = endTime;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public int getCommitCount() {
      return commitCount;
    }

    public void setCommitCount(int commitCount) {
      this.commitCount = commitCount;
    }

    public int getReadCount() {
      return readCount;
    }

    public void setReadCount(int readCount) {
      this.readCount = readCount;
    }

    public int getFilterCount() {
      return filterCount;
    }

    public void setFilterCount(int filterCount) {
      this.filterCount = filterCount;
    }

    public int getWriteCount() {
      return writeCount;
    }

    public void setWriteCount(int writeCount) {
      this.writeCount = writeCount;
    }

    public String getExitCode() {
      return exitCode;
    }

    public void setExitCode(String exitCode) {
      this.exitCode = exitCode;
    }

    public String getExitMessage() {
      return exitMessage;
    }

    public void setExitMessage(String exitMessage) {
      this.exitMessage = exitMessage;
    }

    public int getReadSkipCount() {
      return readSkipCount;
    }

    public void setReadSkipCount(int readSkipCount) {
      this.readSkipCount = readSkipCount;
    }

    public int getWriteSkipCount() {
      return writeSkipCount;
    }

    public void setWriteSkipCount(int writeSkipCount) {
      this.writeSkipCount = writeSkipCount;
    }

    public int getProcessSkipCount() {
      return processSkipCount;
    }

    public void setProcessSkipCount(int processSkipCount) {
      this.processSkipCount = processSkipCount;
    }

    public int getRollbackCount() {
      return rollbackCount;
    }

    public void setRollbackCount(int rollbackCount) {
      this.rollbackCount = rollbackCount;
    }

    public Date getLastUpdated() {
      return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
      this.lastUpdated = lastUpdated;
    }

    /** Creates a persistent model from the business model of a step execution. */
    static MongoStepExecution makeOf(StepExecution stepExecution) {
      final MongoStepExecution mongoStepExecution = new MongoStepExecution();
      mongoStepExecution.setJobExecutionId(stepExecution.getJobExecutionId());
      mongoStepExecution.setStepExecutionId(stepExecution.getId());
      mongoStepExecution.setExitCode(stepExecution.getExitStatus().getExitCode());
      mongoStepExecution.setExitMessage(
          truncateExitDescription(stepExecution.getExitStatus().getExitDescription()));
      mongoStepExecution.setStepName(stepExecution.getStepName());
      mongoStepExecution.setCommitCount(stepExecution.getCommitCount());
      mongoStepExecution.setEndTime(stepExecution.getEndTime());
      mongoStepExecution.setFilterCount(stepExecution.getFilterCount());
      mongoStepExecution.setLastUpdated(stepExecution.getLastUpdated());
      mongoStepExecution.setProcessSkipCount(stepExecution.getProcessSkipCount());
      mongoStepExecution.setReadCount(stepExecution.getReadCount());
      mongoStepExecution.setReadSkipCount(stepExecution.getReadSkipCount());
      mongoStepExecution.setRollbackCount(stepExecution.getRollbackCount());
      mongoStepExecution.setStarTime(stepExecution.getStartTime());
      mongoStepExecution.setEndTime(stepExecution.getEndTime());
      mongoStepExecution.setStatus(stepExecution.getStatus().name());
      mongoStepExecution.setVersion(stepExecution.getVersion());
      mongoStepExecution.setWriteCount(stepExecution.getWriteCount());
      mongoStepExecution.setWriteSkipCount(stepExecution.getWriteSkipCount());

      return mongoStepExecution;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MongoStepExecution that = (MongoStepExecution) o;
      return stepExecutionId == that.stepExecutionId
          && version == that.version
          && jobExecutionId == that.jobExecutionId
          && commitCount == that.commitCount
          && readCount == that.readCount
          && filterCount == that.filterCount
          && writeCount == that.writeCount
          && readSkipCount == that.readSkipCount
          && writeSkipCount == that.writeSkipCount
          && processSkipCount == that.processSkipCount
          && rollbackCount == that.rollbackCount
          && Objects.equals(stepName, that.stepName)
          && Objects.equals(starTime, that.starTime)
          && Objects.equals(endTime, that.endTime)
          && Objects.equals(status, that.status)
          && Objects.equals(exitCode, that.exitCode)
          && Objects.equals(exitMessage, that.exitMessage)
          && Objects.equals(lastUpdated, that.lastUpdated);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          stepExecutionId,
          version,
          stepName,
          jobExecutionId,
          starTime,
          endTime,
          status,
          commitCount,
          readCount,
          filterCount,
          writeCount,
          exitCode,
          exitMessage,
          readSkipCount,
          writeSkipCount,
          processSkipCount,
          rollbackCount,
          lastUpdated);
    }

    @Override
    public String toString() {
      return "MongoStepExecution{"
          + "stepExecutionId="
          + stepExecutionId
          + ", version="
          + version
          + ", stepName='"
          + stepName
          + '\''
          + ", jobExecutionId="
          + jobExecutionId
          + ", starTime="
          + starTime
          + ", endTime="
          + endTime
          + ", status='"
          + status
          + '\''
          + ", commitCount="
          + commitCount
          + ", readCount="
          + readCount
          + ", filterCount="
          + filterCount
          + ", writeCount="
          + writeCount
          + ", exitCode='"
          + exitCode
          + '\''
          + ", exitMessage='"
          + exitMessage
          + '\''
          + ", readSkipCount="
          + readSkipCount
          + ", writeSkipCount="
          + writeSkipCount
          + ", processSkipCount="
          + processSkipCount
          + ", rollbackCount="
          + rollbackCount
          + ", lastUpdated="
          + lastUpdated
          + '}';
    }
  }
}
