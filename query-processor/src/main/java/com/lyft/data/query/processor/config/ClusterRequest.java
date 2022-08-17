package com.lyft.data.query.processor.config;

import lombok.Data;

@Data
public class ClusterRequest {
  private String queryId;
  private String nextUri;
}
