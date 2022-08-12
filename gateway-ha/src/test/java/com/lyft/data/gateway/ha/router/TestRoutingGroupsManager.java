package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.HaGatewayTestUtils;
import com.lyft.data.gateway.ha.config.DataStoreConfiguration;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;

import java.io.File;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import org.testng.annotations.Test;

@Test
public class TestRoutingGroupsManager {
  private RoutingGroupsManager routingGroupsManager;
  private HaGatewayManager haGatewayManager;

  @BeforeClass(alwaysRun = true)
  public void setup() {
    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File tempH2DbDir = new File(baseDir, "h2db-rg-" + System.currentTimeMillis());
    tempH2DbDir.deleteOnExit();

    HaGatewayTestUtils.seedRequiredData(
        new HaGatewayTestUtils.TestConfig("", tempH2DbDir.getAbsolutePath()));
    String jdbcUrl = "jdbc:h2:" + tempH2DbDir.getAbsolutePath();
    DataStoreConfiguration db = new DataStoreConfiguration(jdbcUrl, "sa", "sa", "org.h2.Driver");
    JdbcConnectionManager connectionManager = new JdbcConnectionManager(db);

    haGatewayManager = new HaGatewayManager(connectionManager);
    routingGroupsManager = new RoutingGroupsManager(connectionManager);
  }

  @Test
  public void testAddingRoutingGroup() {
    List<ProxyBackendConfiguration> backends = haGatewayManager.getAllBackends();
    List<RoutingGroupConfiguration> groups = routingGroupsManager.getAllRoutingGroups(backends);

    for (int i = 0; i < 10; i++) {
      RoutingGroupConfiguration rg = new RoutingGroupConfiguration(Integer.toString(i));
      routingGroupsManager.addRoutingGroup(rg);
    }

    groups = routingGroupsManager.getAllRoutingGroups(backends);
    for (int i = 0; i < 10; i++) {
      Assert.assertNotNull(routingGroupsManager.getByName(groups, Integer.toString(i)));
    }
  }
}
