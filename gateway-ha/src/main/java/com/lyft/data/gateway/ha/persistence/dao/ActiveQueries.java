package com.lyft.data.gateway.ha.persistence.dao;

import com.lyft.data.gateway.ha.config.ActiveQueryConfiguration;

import java.util.ArrayList;
import java.util.List;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Cached;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

/**
 * Class used to represent and communicate with the "active_queries" table
 * in mysql.
 */
@Table("active_queries")
@IdName("query_id")
@Cached
public class ActiveQueries extends Model {
  public static final String queryId = "query_id";
  public static final String mappedId = "mapped_id";
  public static final String completed = "completed";

  /**
   * Upcasts a list of ActiveQueries returned from a sql query to a list of
   * active query configurations that contain the same information.
   * @param activeQueryList List of active queries from a sql query
   * @return List of active query configurations
   */
  public static List<ActiveQueryConfiguration> upcast(List<ActiveQueries> activeQueryList) {
    List<ActiveQueryConfiguration> activeQueryConfigurations = new ArrayList<>();
    for (ActiveQueries model : activeQueryList) {
      activeQueryConfigurations.add(new ActiveQueryConfiguration(model.getString(queryId),
          model.getString(mappedId),
          model.getBoolean(completed)));
    }

    return activeQueryConfigurations;
  }

  /**
   * Creates a new row in the active_query table.
   * @param query Query information to create a row
   */
  public static void create(ActiveQueryConfiguration query) {
    ActiveQueries.create(queryId, query.getQueryId(),
        mappedId, query.getMappedId(),
        completed, query.isCompleted()).insert();
  }

  /**
   * Updates information about an active query in the table.
   * @param model Model representing row from sql query
   * @param query Query information to update a row
   */
  public static void update(ActiveQueries model, ActiveQueryConfiguration query) {
    model.set(queryId, query.getQueryId(),
        mappedId, query.getMappedId(),
        completed, query.isCompleted()).saveIt();
  }

  /**
   * Deletes an active query from the table.
   * @param query Query information to delete a row
   */
  public static void delete(ActiveQueryConfiguration query) {
    ActiveQueries.create(queryId, query.getQueryId(),
        mappedId, query.getMappedId(),
        completed, query.isCompleted()).delete();
  }

  /**
   * Deletes a row from the active query table by queryId.
   * @param queryId
   */
  public static void delete(String queryId) {
    ActiveQueries.delete("query_id = ?", queryId);
  }
}
