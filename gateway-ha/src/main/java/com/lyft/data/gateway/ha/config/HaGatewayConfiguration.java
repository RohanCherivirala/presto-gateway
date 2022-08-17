package com.lyft.data.gateway.ha.config;

import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class HaGatewayConfiguration extends QueryProcessorConfiguration {
  private RequestRouterConfiguration requestRouter;
  private NotifierConfiguration notifier;
  private DataStoreConfiguration dataStore;
}
