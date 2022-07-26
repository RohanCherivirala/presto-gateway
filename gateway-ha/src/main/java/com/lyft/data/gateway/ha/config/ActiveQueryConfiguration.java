package com.lyft.data.gateway.ha.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * This class stores information about an active query.
 */
@Data
@AllArgsConstructor
@ToString
public class ActiveQueryConfiguration {
  private String queryId;
  private String mappedId;
  private boolean completed;
}
