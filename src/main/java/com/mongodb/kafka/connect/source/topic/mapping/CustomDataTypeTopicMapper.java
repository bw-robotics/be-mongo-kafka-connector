package com.mongodb.kafka.connect.source.topic.mapping;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.source.MongoSourceConfig;

public class CustomDataTypeTopicMapper implements TopicMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomDataTypeTopicMapper.class);

  public static final String OPERATION_TYPE_FIELD = "operationType";
  public static final String FULL_DOCUMENT_FIELD = "fullDocument";
  public static final String DATA_TYPE_FIELD = "dataType";
  public static final String NAMESPACE_FIELD = "ns";
  public static final String COLLECTION_FIELD = "coll";
  public static final String UPDATED_DESCRIPTION_FIELD = "updateDescription";
  public static final String INSERT_OPERATION_TYPE = "insert";

  private String separator;
  private String prefix;
  private String suffix;
  private String database;
  private String dlqTopicName;

  @Override
  public void configure(final MongoSourceConfig config) {
    final String prefix = config.getString(TOPIC_PREFIX_CONFIG);
    final String suffix = config.getString(TOPIC_SUFFIX_CONFIG);

    this.separator = config.getString(TOPIC_SEPARATOR_CONFIG);
    this.prefix = prefix.isEmpty() ? prefix : prefix + separator;
    this.suffix = suffix.isEmpty() ? suffix : separator + suffix;
    this.database = config.getString(DATABASE_CONFIG);
    this.dlqTopicName = config.getString(OVERRIDE_ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
  }

  /**
   * Dynamically builds and returns the topic name from changeStreamDocument param.
   * In the case of operationType=INSERT, DATA_TYPE_FIELD field is located in the FULL_DOCUMENT_FIELD of the Document.
   * We are doing this only for operationType=INSERT, because it should be triggered only for certain collections where
   * we don't have any other operation type:
   * 1. Event
   * 2. Command
   * In the future, event if we do update operations on top of these collections, dataType of the documents will never
   * be changed, so we will be safe.
   * @param changeStreamDocument A document produced by a MongoDB change stream with accordance to
   *     the <a
   *     href="https://www.mongodb.com/docs/kafka-connector/current/source-connector/configuration-properties/change-stream/">
   *     change stream configuration</a> and the <a
   *     href="https://www.mongodb.com/docs/kafka-connector/current/source-connector/configuration-properties/startup/">startup
   *     configuration</a>.
   *
   * @return
   */
  @Override
  public String getTopic(final BsonDocument changeStreamDocument) {
    final boolean isValidJsonFields = isValidJsonFields(changeStreamDocument);

    if (!isValidJsonFields) {
      LOGGER.error("Json fields are not valid in document: {}", changeStreamDocument);
      return dlqTopicName;
    }

    try {
      final BsonDocument nsDocument = changeStreamDocument.get(NAMESPACE_FIELD).asDocument();
      final String collection = nsDocument.get(COLLECTION_FIELD).asString().getValue();
      final String operationType =
          changeStreamDocument.get(OPERATION_TYPE_FIELD).asString().getValue();

      if (!INSERT_OPERATION_TYPE.equals(operationType)) {
        return getTopicName(collection);
      }

      final BsonDocument fullDocument = changeStreamDocument.get(FULL_DOCUMENT_FIELD).asDocument();
      if (!fullDocument.containsKey(DATA_TYPE_FIELD)) {
        return getTopicName(collection);
      }

      final String dataType = fullDocument.get(DATA_TYPE_FIELD).asString().getValue();
      return getTopicName(dataType, collection);
    } catch (Exception e) {
      LOGGER.error(
          "Exception occurred while trying to get topic name for document: {}",
          changeStreamDocument,
          e);
      return dlqTopicName;
    }
  }

  private boolean isValidJsonFields(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey(OPERATION_TYPE_FIELD)) {
      return false;
    }

    if (!changeStreamDocument.containsKey(FULL_DOCUMENT_FIELD)
        && !changeStreamDocument.containsKey(UPDATED_DESCRIPTION_FIELD)) {
      return false;
    }

    if (!changeStreamDocument.containsKey(NAMESPACE_FIELD)) {
      return false;
    }

    return true;
  }

  private String getTopicName(final String collection) {
    return String.join("", prefix, database, separator, collection, suffix);
  }

  private String getTopicName(final String dataType, final String collection) {
    return String.join("", prefix, database, separator, collection, separator, dataType, suffix);
  }
}
