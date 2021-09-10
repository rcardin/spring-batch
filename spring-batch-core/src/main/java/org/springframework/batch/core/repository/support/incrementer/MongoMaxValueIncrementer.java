package org.springframework.batch.core.repository.support.incrementer;

import com.mongodb.client.MongoClient;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.support.incrementer.DataFieldMaxValueIncrementer;

/**
 * @see <a href="https://www.mongodb.com/blog/post/generating-globally-unique-identifiers-for-use-with-mongodb">Generating Globally Unique Identifiers for Use with MongoDB</a>
 */
public class MongoMaxValueIncrementer implements DataFieldMaxValueIncrementer {
  
  private final MongoOperations mongoOperations;
  private final String collectionName;
  
  public MongoMaxValueIncrementer(
      MongoClient mongoClient,
      String databaseName,
      String collectionName) {
    this.mongoOperations = new MongoTemplate(mongoClient, databaseName);
    this.collectionName = collectionName;
  }
  
  @Override
  public int nextIntValue() throws DataAccessException {
    return 0;
  }
  
  @Override
  public long nextLongValue() throws DataAccessException {
    // TODO Add the write concern?
    final UniqueIdentifierCounter counter = mongoOperations.findAndModify(
        Query.query(Criteria.where("_id").is("UNIQUE_COUNT_DOCUMENT_IDENTIFIER")),
        new Update()
            .inc("COUNT", 1)
            .setOnInsert("NOTE",
                "Increment COUNT using findAndModify to ensure that the COUNT field will be incremented atomically with the fetch of this document"),
        FindAndModifyOptions.none(),
        UniqueIdentifierCounter.class,
        collectionName
    );
    // TODO NPE?
    return counter.getCount();
  }
  
  @Override
  public String nextStringValue() throws DataAccessException {
    return null;
  }
  
  @Document
  static class UniqueIdentifierCounter {
    @MongoId
    private final String id;
    @Field("COUNT")
    private final long count;
    @Field("NOTE")
    private final String note;
  
    UniqueIdentifierCounter(String id, long count, String note) {
      this.id = id;
      this.count = count;
      this.note = note;
    }
  
    long getCount() {
      return count;
    }
  }
}
