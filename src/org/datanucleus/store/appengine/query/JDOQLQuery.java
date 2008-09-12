// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import org.datanucleus.ObjectManager;
import org.datanucleus.store.query.AbstractJDOQLQuery;

import java.util.List;
import java.util.Map;


/**
 * Implementation of JDOQL for the app engine datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLQuery extends AbstractJDOQLQuery {

  /**
   * The underlying Datastore query implementation.
   */
  private final DatastoreQuery datastoreQuery;

  /**
   * Constructs a new query instance that uses the given object manager.
   *
   * @param om The associated ObjectManager for this query.
   */
  public JDOQLQuery(ObjectManager om) {
    this(om, (JDOQLQuery) null);
  }

  /**
   * Constructs a new query instance having the same criteria as the given query.
   *
   * @param om The ObjectManager.
   * @param q The query from which to copy criteria.
   */
  public JDOQLQuery(ObjectManager om, JDOQLQuery q) {
    super(om, q);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
   *
   * @param om The object manager.
   * @param query The JDOQL query string.
   */
  public JDOQLQuery(ObjectManager om, String query) {
    super(om, query);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected List<?> performExecute(Map parameters) {
    @SuppressWarnings("unchecked")
    Map<String, ?> params = parameters;
    return datastoreQuery.performExecute(LOCALISER, compilation, fromInclNo, toExclNo, params);
  }

  // Exposed for tests.
  DatastoreQuery getDatastoreQuery() {
    return datastoreQuery;
  }
}