package org.springframework.batch.core.repository.support;

import com.mongodb.client.MongoClient;

import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.MongoExecutioncontextDao;
import org.springframework.batch.core.repository.dao.MongoJobExecutionDao;
import org.springframework.batch.core.repository.dao.MongoJobInstanceDao;
import org.springframework.batch.core.repository.dao.MongoStepExecutionDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.incrementer.MongoMaxValueIncrementer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

public class MongoJobRepositoryFactoryBean extends AbstractJobRepositoryFactoryBean implements InitializingBean {

    private MongoClient mongoClient;

    private String databaseName;

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    protected JobInstanceDao createJobInstanceDao() throws Exception {
        return new MongoJobInstanceDao(this.mongoClient, this.databaseName);
    }

    @Override
    protected JobExecutionDao createJobExecutionDao() throws Exception {
        return new MongoJobExecutionDao(this.mongoClient, this.databaseName);
    }

    @Override
    protected StepExecutionDao createStepExecutionDao() throws Exception {
        final MongoStepExecutionDao mongoStepExecutionDao = new MongoStepExecutionDao(
            this.mongoClient,
            this.databaseName
        );
        final MongoMaxValueIncrementer stepExecutionIncrementer = new MongoMaxValueIncrementer(
            this.mongoClient,
            this.databaseName,
            "STEP_EXECUTION_SEQ"
        );
        mongoStepExecutionDao.setStepExecutionIncrementer(stepExecutionIncrementer);
        mongoStepExecutionDao.afterPropertiesSet();
        return mongoStepExecutionDao;
    }

    @Override
    protected ExecutionContextDao createExecutionContextDao() throws Exception {
        return new MongoExecutioncontextDao(this.mongoClient, this.databaseName);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(this.mongoClient, "MongoClient must not be null.");
        Assert.notNull(this.databaseName, "Database name must not be null.");
    }
}
