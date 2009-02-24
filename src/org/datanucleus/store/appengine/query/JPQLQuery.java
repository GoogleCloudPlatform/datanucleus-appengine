/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
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
