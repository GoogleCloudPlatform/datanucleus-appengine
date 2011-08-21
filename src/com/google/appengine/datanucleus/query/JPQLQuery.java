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
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.metadata.AbstractClassMetaData;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.MetaDataUtils;

import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.Extent;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.QueryInvalidParametersException;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.Iterator;
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
   * Constructs a new query instance that uses the given StoreManager and ExecutionContext.
   * @param storeMgr StoreManager
   * @param ec ExecutionContext
   */
  public JPQLQuery(StoreManager storeMgr, ExecutionContext ec) {
    this(storeMgr, ec, (JPQLQuery) null);
  }

  /**
   * Constructs a new query instance having the same criteria as the given query.
   * @param storeMgr StoreManager
   * @param ec ExecutionContext
   * @param q The query from which to copy criteria.
   */
  public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q) {
    super(storeMgr, ec, q);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * Constructor for a JPQL query where the query is specified using the "Single-String" format.
   * @param storeMgr StoreManager
   * @param ec ExecutionContext
   * @param query The JPQL query string.
   */
  public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
    super(storeMgr, ec, query);
    datastoreQuery = new DatastoreQuery(this);
  }

  /**
   * Convenience method to return whether the query should be evaluated in-memory.
   * @return Use in-memory evaluation?
   */
  protected boolean evaluateInMemory() {
    if (candidateCollection != null || candidateExtent != null) {
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
    long startTime = System.currentTimeMillis();
    if (NucleusLogger.QUERY.isDebugEnabled()) {
      NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JPQL", getSingleStringQuery(), null));
    }

    if (type == Query.BULK_UPDATE) {
      // TODO Support this
      throw new NucleusException("JPQL Bulk UPDATE is not yet supported");
    }

    Object results = null;
    if (evaluateInMemory()) {
      // Evaluating in-memory so build up list of candidates
      List candidates = null;
      if (candidateCollection != null) {
        candidates = new ArrayList(candidateCollection);
      }
      else if (candidateExtent != null) {
        candidates = new ArrayList();
        Iterator iter = candidateExtent.iterator();
        while (iter.hasNext()) {
          candidates.add(iter.next());
        }
      }
      else {
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
      ManagedConnection mconn = getStoreManager().getConnection(ec);
      try {
        results = datastoreQuery.performExecute(mconn, LOCALISER, compilation, parameters, false);
      } finally {
//        mconn.release();
      }
    }

    if (NucleusLogger.QUERY.isDebugEnabled()) {
      NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JPQL", 
          "" + (System.currentTimeMillis() - startTime)));
    }

    return results;
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
  public void setUnique(boolean unique) {
    // Workaround a DataNucleus bug.
    // The superclass implementation discards the comiled query when this is set,
    // but since jpql param values are set _before_ the query is executed,
    // discarding the compiled query discards the parameter values as well and
    // we have no way of getting them back.
    this.unique = unique;
  }

  @Override
  protected void checkParameterTypesAgainstCompilation(Map parameterValues) {
    // Disabled as part of our DataNuc 1.1.3 upgrade so that we can be 
    // continue to allow multi-value properties and implicit conversions.

    // TODO(maxr) Re-enable the checks that don't break multi-value filters
    // and implicit conversions.
  }

  @Override
  protected void applyImplicitParameterValueToCompilation(String name, Object value) {
    try {
      super.applyImplicitParameterValueToCompilation(name, value);
    } catch (QueryInvalidParametersException e) {
      // swallow this exception - need to disable the type checking so we can
      // be friendly about implicit conversions
    }
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
            "SINGLE_TABLE interitance mapping strategy.");
      }
    }
    super.setSubclasses(subclasses);
  }
}
