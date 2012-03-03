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

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;

import org.datanucleus.exceptions.NucleusUserException;

import com.google.appengine.datanucleus.Utils.Function;

import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import javax.jdo.JDOUserException;

/**
 * An {@link AbstractQueryResult} implementation that streams results, converting
 * from {@link Entity Entities} to POJOs as clients access the data.
 *
 * @author Max Ross <maxr@google.com>
 */
class StreamingQueryResult extends AbstractQueryResult {

  /** Delegate for lazy loading of results. */
  private final LazyResult<Object> lazyResult;

  private boolean loadResultsAtCommit = true;

  private final Cursor endCursor;

  private boolean hasError;

  private RuntimeExceptionWrappingIterable inputIterable;

  /**
   * Constructs a StreamingQueryResult.
   * @param query The query which yields the results.
   * @param lazyEntities The result of the query.
   * @param entityToPojoFunc A function that can convert a {@link Entity} into a pojo.
   * @param endCursor Provides a cursor that points to the end of the result set. Can be null.
   */
  public StreamingQueryResult(Query query, Iterable<Entity> lazyEntities,
      Function<Entity, Object> entityToPojoFunc, Cursor endCursor) {
    super(query);
    if (lazyEntities instanceof RuntimeExceptionWrappingIterable) {
      this.inputIterable = (RuntimeExceptionWrappingIterable) lazyEntities;
    }
    this.lazyResult = new LazyResult<Object>(lazyEntities, entityToPojoFunc, query.useResultsCaching());
    this.endCursor = endCursor;
  }

  @Override
  public void disconnect() {
    if (inputIterable != null) {
      // Relay the iterable error out
      this.hasError = inputIterable.hasError();
    }

    super.disconnect();
  }

  @Override
  protected void closingConnection() {
    // Connection is being closed so last chance to grab any results not yet loaded
    if (loadResultsAtCommit && isOpen()) {
      if (hasError) {
        NucleusLogger.QUERY.info("Skipping resolution of remaining results due to earlier error.");
      } else {
        try {
          // If we are still open, force consumption of the rest of the results
          lazyResult.resolveAll();
          // TODO Get rid of this selective exception swallowing. Makes no sense
        } catch (NucleusUserException jpue) {
          // Log any exception - can get exceptions when maybe the user has specified an invalid result class etc
          NucleusLogger.QUERY.warn("Exception thrown while loading remaining rows of query : " + jpue.getMessage());
        } catch (JDOUserException ue) {
          // Log any exception - can get exceptions when maybe the user has specified an invalid result class etc
          NucleusLogger.QUERY.warn("Exception thrown while loading remaining rows of query : " + ue.getMessage());
        }
      }

      // Cache the query results (if required)
      cacheQueryResults();
    }
  }

  @Override
  protected void closeResults() {
    // Cache the query results (if required)
    cacheQueryResults();
  }

  /**
   * Method to cache the results (List of the Entity keys) if it has been requested. 
   */
  protected void cacheQueryResults() {
    if (query.useResultsCaching()) {
      lazyResult.resolveAll();
      query.getQueryManager().addDatastoreQueryResult(query, query.getInputParameters(), lazyResult.getEntityKeys());
    }
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public Object get(int index) {
    return lazyResult.get(index);
  }

  /**
   * @throws NoSuchElementException if there are no more elements to resolve.
   */
  void resolveNext() {
    lazyResult.resolveNext();
  }

  @Override
  public Iterator<Object> iterator() {
    return lazyResult.listIterator();
  }

  @Override
  public ListIterator<Object> listIterator() {
    return lazyResult.listIterator();
  }

  @Override
  public int size() {
    return lazyResult.size();
  }

  Cursor getEndCursor() {
    return endCursor;
  }
}