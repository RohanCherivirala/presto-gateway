package com.lyft.data.query.processor.error;

import com.lyft.data.query.processor.config.PrestoError;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class serves as a manager for handling produced errors.
 */
public class ErrorManager {
  private ArrayList<PrestoError> errorsToRetry;

  /**
   * Create list of errors to retry.
   */
  public ErrorManager() {
    errorsToRetry = new ArrayList<>();

    // Standard errors
    errorsToRetry.addAll(Arrays.asList(
        new PrestoError("SERVER_STARTING_UP", 65548),
        new PrestoError("REMOTE_HOST_GONE", 65558),
        new PrestoError("REMOTE_TASK_FAILED", 65563),
        new PrestoError("QUERY_QUEUE_FULL", 131074)));

    // Hive errors
    int hiveErrorCodeBase = 0x0100_0000;
    errorsToRetry.addAll(Arrays.asList(
        new PrestoError("HIVE_METASTORE_ERROR", hiveErrorCodeBase)));
  }

  /**
   * Check if the specific error should be retried.
   * 
   * @param errorCode Error code produced
   * @param errorName Name of error
   * @return If the error should be retired
   */
  public boolean shouldRetry(int errorCode, String errorName) {
    for (PrestoError error : errorsToRetry) {
      if (error.getErrorName().equals(errorName) || error.getErrorCode() == errorCode) {
        return true;
      }
    }

    return false;
  }
}
