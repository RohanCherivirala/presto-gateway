package com.lyft.data.gateway.ha.config;

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
}
