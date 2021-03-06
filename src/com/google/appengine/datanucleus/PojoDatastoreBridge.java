/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.metadata.AbstractClassMetaData;

import com.google.appengine.datanucleus.query.DatastoreQuery;

import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.store.query.AbstractQueryResult;

import java.util.List;
import java.util.Map;

/**
 * Utilities for converting between the low-level datastore api and pojos.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
class PojoDatastoreBridge {

  <T> List<T> toPojoResult(final ExecutionContext ec, Class<T> cls, Iterable<Entity> queryResultIterable, Cursor endCursor) {
    final ClassLoaderResolver clr = ec.getClassLoaderResolver();
    final AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(cls, clr);
    Utils.Function<Entity, Object> func = new Utils.Function<Entity, Object>() {
      public Object apply(Entity from) {
        return EntityUtils.entityToPojo(from, acmd, clr, ec, true, ec.getFetchPlan().getCopy());
      }
    };

    AbstractJavaQuery query = new DummyQuery(ec.getStoreManager(), ec);
    final ManagedConnection mconn = ec.getStoreManager().getConnection(ec);
    try {
      List<T> results = (List<T>) DatastoreQuery.newStreamingQueryResultForEntities(
          queryResultIterable, func, endCursor, query);

      if (results instanceof AbstractQueryResult) {
        // Lazy loading results : add listener to the connection so we can get a callback when the connection is flushed.
        final AbstractQueryResult qr = (AbstractQueryResult)results;
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
      return results;
    } finally {
      mconn.release();
    }
  }

  private static final class DummyQuery extends AbstractJavaQuery {

    private DummyQuery(StoreManager storeMgr, ExecutionContext ec) {
      super(storeMgr, ec);
    }

    public String getSingleStringQuery() {
      throw new UnsupportedOperationException();
    }

    protected void compileInternal(Map parameterValues) {
      throw new UnsupportedOperationException();
    }

    protected Object performExecute(Map parameters) {
      throw new UnsupportedOperationException();
    }
  }
}