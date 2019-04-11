package edu.illinois.storm;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

public class TopNStoreMapper implements RedisStoreMapper {
  private RedisDataTypeDescription description;
  private final String hashKey;

  public TopNStoreMapper(String hashKey) {
    this.hashKey = hashKey;
    description =
        new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
  }

  @Override
  public RedisDataTypeDescription getDataTypeDescription() {
    return description;
  }

  @Override
  public String getKeyFromTuple(ITuple tuple) {
    /* ----------------------TODO-----------------------
    Task: define which part of the tuple as the key
		Hint: the key for this part is always "top-N"
    ------------------------------------------------- */
		return "";
		// End
  }

  @Override
  public String getValueFromTuple(ITuple tuple) {
    /* ----------------------TODO-----------------------
    Task: define which part of the tuple as the value
    ------------------------------------------------- */
		return "";
		// End
  }
}
