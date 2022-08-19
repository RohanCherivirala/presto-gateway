package com.lyft.data.query.processor.config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ClusterRequest {
  private String queryId;
  private String nextUri;
}