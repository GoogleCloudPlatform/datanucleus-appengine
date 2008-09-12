package org.datanucleus.store.appengine.query;

import org.datanucleus.ObjectManager;
import org.datanucleus.store.query.AbstractJPQLQuery;

import java.util.List;
import java.util.Map;

/**
 * Implementation of JPQL for the app engine datastore.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 */
public class JPQLQuery extends AbstractJPQLQuery {

  /**
   * The underlying Datastore query implementation.
   */
  private final DatastoreQuery datastoreQuery;

  /**
   * Constructs a new query instance that uses the given object manager.
   *
   * @param om The associated ObjectManager for this query.
   */
  public JPQLQuery(ObjectManager om) {
    this(om, (JPQLQuery) null);
  }

  /**
   * Constructs a new query instance having the same criteria as the given
   * query.
   *
   * @param om The ObjectManager.
   * @param q The query from which to copy criteria.
   */
  public JPQLQuery(ObjectManager om, JPQLQuery q) {
    super(om, q);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * Constructor for a JPQL query where the query is specified using the
   * "Single-String" format.
   *
   * @param om The object manager.
   * @param query The JPQL query string.
   */
  public JPQLQuery(ObjectManager om, String query) {
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
