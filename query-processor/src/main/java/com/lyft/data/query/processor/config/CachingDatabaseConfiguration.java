package com.lyft.data.query.processor.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CachingDatabaseConfiguration {
  private String databaseType;
  private String host;
  private int port;
  private String bucket;
}
