package com.lyft.data.server.config;

import lombok.Data;

@Data
public class GatewayServerConfiguration {
  private String name;
  private int localPort;
  private String proxyTo;
  private String prefix = "/";
  private String trustAll = "true";
  private String preserveHost = "true";
  private boolean ssl;
  private String keystorePath;
  private String keystorePass;
}
