package com.lyft.data.query.processor.config;

import com.lyft.data.baseapp.AppConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class QueryProcessorConfiguration extends AppConfiguration {
  private CachingDatabaseConfiguration cachingDatabase;
}
