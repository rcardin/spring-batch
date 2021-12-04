package org.springframework.batch.core.repository.dao;

import com.mongodb.client.MongoClient;
import com.mongodb.client.result.UpdateResult;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.support.incrementer.MongoMaxValueIncrementer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;

/**
 * @author Mahmoud Ben Hassine
 * @author Riccardo Cardin
 * @author Giuseppe Pavan
 */
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
    final MongoStepExecution mongoStepExecution = buildMongoStepExecution(stepExecution);

    mongoOperations.insert(mongoStepExecution, COLLECTION_NAME);
  }

  private MongoStepExecution buildMongoStepExecution(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    Assert.isNull(
        stepExecution.getId(),
        "to-be-saved (not updated) StepExecution can't already have an id assigned");
    Assert.isNull(
        stepExecution.getVersion(),
        "to-be-saved (not updated) StepExecution can't already have a version assigned");
    stepExecution.setId(stepExecutionIncrementer.nextLongValue());
    stepExecution.incrementVersion(); // Should be 0
    return MongoStepExecution.makeOf(stepExecution);
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
  public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
    if (!stepExecutions.isEmpty()) {
      final List<MongoStepExecution> mongoStepExecutions =
          stepExecutions
              .stream()
              .map(this::buildMongoStepExecution)
              .collect(Collectors.toList());
      mongoOperations.insert(mongoStepExecutions, COLLECTION_NAME);
    }
  }

  @Override
  public void updateStepExecution(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    Assert.notNull(stepExecution.getId(),
        "StepExecution Id cannot be null. StepExecution must saved"
            + " before it can be updated.");
    final String exitDescription = truncateExitDescription(stepExecution
        .getExitStatus()
        .getExitDescription());

    synchronized (stepExecution) {
      int version = stepExecution.getVersion() + 1;
      final UpdateResult updateResult = mongoOperations.updateFirst(new Query(Criteria
              .where("id")
              .is(stepExecution.getId())
              .and("version")
              .is(stepExecution.getVersion())),
          new Update()
              .set("startTime", stepExecution.getStartTime())
              .set("endTime", stepExecution.getEndTime())
              .set("status", stepExecution
                  .getStatus()
                  .toString())
              .set("commitCount", stepExecution.getCommitCount())
              .set("readCount", stepExecution.getReadCount())
              .set("filterCount", stepExecution.getFilterCount())
              .set("writeCount", stepExecution.getWriteCount())
              .set("exitCode", stepExecution
                  .getExitStatus()
                  .getExitCode())
              .set("exitDescription", exitDescription)
              .set("version", version)
              .set("readSkipCount", stepExecution.getReadSkipCount())
              .set("processSkipCount", stepExecution.getProcessSkipCount())
              .set("writeSkipCount", stepExecution.getWriteSkipCount())
              .set("rollbackCount", stepExecution.getRollbackCount())
              .set("lastUpdated", stepExecution.getLastUpdated()),
          StepExecution.class,
          COLLECTION_NAME);

      if (updateResult.getModifiedCount() == 0) {
        final StepExecution currentStepExecution = mongoOperations
            .findOne(
                new Query(Criteria
                    .where("_id")
                    .is(stepExecution.getId())),
                StepExecution.class, COLLECTION_NAME);
        Assert.notNull(currentStepExecution,
            "Can't find a step execution with id=" + stepExecution.getId());
        int currentVersion = currentStepExecution.getVersion();
        throw new OptimisticLockingFailureException("Attempt to update step execution id="
            + stepExecution.getId() + " with wrong version (" + stepExecution.getVersion()
            + "), where current version is " + currentVersion);
      }
      stepExecution.incrementVersion();
    }
  }

  @Override
  public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
    final Long jobExecutionId = jobExecution.getId();
    final List<MongoStepExecution> executions = mongoOperations.find(
        Query.query(
            Criteria.where("_id").is(stepExecutionId).and("jobExecutionId").is(jobExecutionId)
        ),
        MongoStepExecution.class,
        COLLECTION_NAME
    );
  
    Assert.state(executions.size() <= 1,
        "There can be at most one step execution with given name for single job execution");
    if (executions.isEmpty()) {
      return null;
    } else {
      return executions.get(0).toStepExecution(jobExecution);
    }
  }

  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    final Long jobInstanceId = jobInstance.getId();
//    mongoOperations.findOne(
//        Query.query(Criteria.where("_id").is(jobInstanceId)),
//    )
    return null;
  }

  @Override
  public void addStepExecutions(JobExecution jobExecution) {
  }

  @Override
  public int countStepExecutions(JobInstance jobInstance, String stepName) {
    return 0;
  }

  /**
   * Mongo representation of the object (aka, persistent model)
   */
  @Document
  static class MongoStepExecution {

    @Id
    private long stepExecutionId;
    private int version;
    private String stepName;
    private long jobExecutionId;
    private long jobInstanceId;
    private Date startTime;
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

    public Date getStartTime() {
      return startTime;
    }

    public void setStartTime(Date startTime) {
      this.startTime = startTime;
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
  
    public long getJobInstanceId() {
      return jobInstanceId;
    }
  
    public void setJobInstanceId(long jobInstanceId) {
      this.jobInstanceId = jobInstanceId;
    }
  
    /**
     * Creates a persistent model from the business model of a step execution.
     */
    static MongoStepExecution makeOf(StepExecution stepExecution) {
      final MongoStepExecution mongoStepExecution = new MongoStepExecution();
      mongoStepExecution.setJobExecutionId(stepExecution.getJobExecutionId());
      mongoStepExecution.setJobInstanceId(
          stepExecution.getJobExecution().getJobInstance().getInstanceId());
      mongoStepExecution.setStepExecutionId(stepExecution.getId());
      mongoStepExecution.setExitCode(stepExecution
          .getExitStatus()
          .getExitCode());
      mongoStepExecution.setExitMessage(
          truncateExitDescription(stepExecution
              .getExitStatus()
              .getExitDescription()));
      mongoStepExecution.setStepName(stepExecution.getStepName());
      mongoStepExecution.setCommitCount(stepExecution.getCommitCount());
      mongoStepExecution.setEndTime(stepExecution.getEndTime());
      mongoStepExecution.setFilterCount(stepExecution.getFilterCount());
      mongoStepExecution.setLastUpdated(stepExecution.getLastUpdated());
      mongoStepExecution.setProcessSkipCount(stepExecution.getProcessSkipCount());
      mongoStepExecution.setReadCount(stepExecution.getReadCount());
      mongoStepExecution.setReadSkipCount(stepExecution.getReadSkipCount());
      mongoStepExecution.setRollbackCount(stepExecution.getRollbackCount());
      mongoStepExecution.setStartTime(stepExecution.getStartTime());
      mongoStepExecution.setEndTime(stepExecution.getEndTime());
      mongoStepExecution.setStatus(stepExecution
          .getStatus()
          .name());
      mongoStepExecution.setVersion(stepExecution.getVersion());
      mongoStepExecution.setWriteCount(stepExecution.getWriteCount());
      mongoStepExecution.setWriteSkipCount(stepExecution.getWriteSkipCount());

      return mongoStepExecution;
    }
  
    /**
     * Creates a business model from the persistent model of a step execution.
     */
    StepExecution toStepExecution(JobExecution jobExecution) {
      StepExecution stepExecution = new StepExecution(stepName, jobExecution, stepExecutionId);
      stepExecution.setCommitCount(commitCount);
      stepExecution.setEndTime(endTime);
      stepExecution.setExitStatus(new ExitStatus(exitCode, exitMessage));
      stepExecution.setFilterCount(filterCount);
      stepExecution.setLastUpdated(lastUpdated);
      stepExecution.setProcessSkipCount(processSkipCount);
      stepExecution.setReadCount(readCount);
      stepExecution.setReadSkipCount(readSkipCount);
      stepExecution.setRollbackCount(rollbackCount);
      stepExecution.setStartTime(startTime);
      stepExecution.setStatus(BatchStatus.valueOf(status));
      stepExecution.setVersion(version);
      stepExecution.setWriteCount(writeCount);
      stepExecution.setWriteSkipCount(writeSkipCount);
      return stepExecution;
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
          && Objects.equals(startTime, that.startTime)
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
          startTime,
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
          + startTime
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
