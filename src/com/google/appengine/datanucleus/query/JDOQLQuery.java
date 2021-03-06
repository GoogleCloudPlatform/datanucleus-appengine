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
package com.google.appengine.datanucleus.query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.metadata.AbstractClassMetaData;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.MetaDataUtils;

import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.Extent;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.CandidateIdsQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of JDOQL for the app engine datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLQuery extends AbstractJDOQLQuery {
  /** The underlying Datastore query implementation. */
  private final DatastoreQuery datastoreQuery;

  /**
   * Constructs a new query instance that uses the given StoreManager and ExecutionContext.
   * @param storeMgr StoreManager
   * @param ec ExecutionContext
   */
  public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec) {
    this(storeMgr, ec, (JDOQLQuery) null);
  }

  /**
   * Constructs a new query instance having the same criteria as the given query.
   * @param storeMgr StoreManager
   * @param ec ExecutionContext
   * @param q The query from which to copy criteria.
   */
  public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, JDOQLQuery q) {
    super(storeMgr, ec, q);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
   * @param storeMgr StoreManager
   * @param ec ExecutionContext.
   * @param query The JDOQL query string.
   */
  public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
    super(storeMgr, ec, query);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * Convenience method to return whether the query should be evaluated in-memory.
   * @return Use in-memory evaluation?
   */
  protected boolean evaluateInMemory() {
    if (candidateCollection != null) {
      if (compilation != null && compilation.getSubqueryAliases() != null) {
        // TODO In-memory evaluation of subqueries isn't fully implemented yet, so remove this when it is
        NucleusLogger.QUERY.warn("In-memory evaluator doesn't currently handle subqueries completely so evaluating in datastore");
        return false;
      }

      // Return true unless the user has explicitly said no
      Object val = getExtension(EXTENSION_EVALUATE_IN_MEMORY);
      if (val == null) {
        return true;
      }
      return Boolean.valueOf((String)val);
    }
    return super.evaluateInMemory();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object performExecute(Map parameters) {
    if (type == org.datanucleus.store.query.Query.BULK_UPDATE) {
      throw new NucleusFatalUserException("Bulk Update statements are not supported.");
    }

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.QUERY.isDebugEnabled()) {
        NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL", getSingleStringQuery(), null));
    }

    if (candidateCollection == null &&
        type == Query.SELECT && resultClass == null && result == null) {
      // Check for cached query results
      List<Object> cachedResults = getQueryManager().getDatastoreQueryResult(this, parameters);
      if (cachedResults != null) {
        // Query results are cached, so return those
        return new CandidateIdsQueryResult(this, cachedResults);
      }
    }

    Object results = null;
    if (evaluateInMemory()) {
        // Evaluating in-memory so build up list of candidates
        List candidates = null;
        if (candidateCollection != null) {
          candidates = new ArrayList(candidateCollection);
        } else {
          Extent ext = getStoreManager().getExtent(ec, candidateClass, subclasses);
          candidates = new ArrayList();
          Iterator iter = ext.iterator();
          while (iter.hasNext()) {
            candidates.add(iter.next());
          }
        }

        // Evaluate in-memory over the candidate instances
        JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, candidates, compilation,
            parameters, ec.getClassLoaderResolver());
        results = resultMapper.execute(true, true, true, true, true);
    }
    else {
      // Evaluate in-datastore
      boolean inmemoryWhenUnsupported = getEvaluateInMemoryWhenUnsupported();
      QueryData qd = datastoreQuery.compile(compilation, parameters, inmemoryWhenUnsupported);
      if (NucleusLogger.QUERY.isDebugEnabled()) {
        // Log the query
        NucleusLogger.QUERY.debug("Query compiled as : " + qd.getDatastoreQueryAsString());
      }

      results = datastoreQuery.performExecute(qd);

      boolean filterInMemory = false;
      boolean orderInMemory = false;
      boolean resultInMemory = (result != null || grouping != null || having != null || resultClass != null);
      if (inmemoryWhenUnsupported) {
        // Set filter/order flags according to what the query can manage in-datastore
        filterInMemory = !datastoreQuery.isFilterComplete();

        if (ordering != null) {
          if (filterInMemory) {
            orderInMemory = true;
          } else {
            if (!datastoreQuery.isOrderComplete()) {
              orderInMemory = true;
            }
          }
        }
      }

      // Evaluate any remaining parts in-memory
      if (filterInMemory || resultInMemory || orderInMemory) {
        JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, (List)results, compilation,
            parameters, ec.getClassLoaderResolver());
        results = resultMapper.execute(filterInMemory, orderInMemory, 
            resultInMemory, resultClass != null, false);
      }

      if (results instanceof AbstractQueryResult) {
        // Lazy loading results : add listener to the connection so we can get a callback when the connection is flushed.
        final AbstractQueryResult qr = (AbstractQueryResult)results;
        final ManagedConnection mconn = getStoreManager().getConnection(ec);
        ManagedConnectionResourceListener listener = new ManagedConnectionResourceListener() {
          public void managedConnectionPreClose() {
            // Disconnect the query from this ManagedConnection (read in unread rows etc)
            qr.disconnect();
          }
          public void managedConnectionPostClose() {}
          public void resourcePostClose() {
            mconn.removeListener(this);
          }
          public void transactionFlushed() {}
          public void transactionPreClose() {
            // Disconnect the query from this ManagedConnection (read in unread rows etc)
            qr.disconnect();
          }
        };
        mconn.addListener(listener);
        qr.addConnectionListener(listener);
      }
    }

    if (NucleusLogger.QUERY.isDebugEnabled()) {
      NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", "" + (System.currentTimeMillis() - startTime)));
    }

    return results;
  }

  boolean getEvaluateInMemoryWhenUnsupported() {
    // Use StoreManager setting and allow override in query extensions
    boolean inmemory = storeMgr.getBooleanProperty("datanucleus.appengine.query.inMemoryWhenUnsupported");
    return getBooleanExtensionProperty(DatastoreManager.QUERYEXT_INMEMORY_WHEN_UNSUPPORTED, inmemory);
  }

  // Exposed for tests.
  DatastoreQuery getDatastoreQuery() {
    return datastoreQuery;
  }

  @Override
  protected boolean supportsTimeout() {
    return true; // GAE/J Datastore supports timeouts
  }

  @Override
  protected void checkParameterTypesAgainstCompilation(Map parameterValues) {
    // Disabled as part of our DataNuc 1.1.3 upgrade so that we can be
    // continue to allow multi-value properties and implicit conversions.

    // TODO(maxr) Re-enable the checks that don't break multi-value filters
    // and implicit conversions.
  }

  @Override
  public void setSubclasses(boolean subclasses) {
    // TODO Enable this!
    // We support only queries that also return subclasses if all subclasses belong to the same kind.
    if (subclasses) {
      DatastoreManager storeMgr = (DatastoreManager) ec.getStoreManager();
      ClassLoaderResolver clr = ec.getClassLoaderResolver();
      AbstractClassMetaData acmd = storeMgr.getMetaDataManager().getMetaDataForClass(getCandidateClass(), clr);
      if (!MetaDataUtils.isNewOrSuperclassTableInheritanceStrategy(acmd)) {
        throw new NucleusFatalUserException(
            "The App Engine datastore only supports queries that return subclass entities with the " +
            "superclass-table interitance mapping strategy.");
      }
    }
    super.setSubclasses(subclasses);
  }
}
