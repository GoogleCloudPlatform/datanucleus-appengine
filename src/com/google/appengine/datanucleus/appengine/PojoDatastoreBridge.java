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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.appengine.query.DatastoreQuery;
import org.datanucleus.store.query.AbstractJavaQuery;

import java.util.List;
import java.util.Map;

/**
 * Utilities for converting between the low-level datastore api and pojos.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
class PojoDatastoreBridge {

  <T> List<T> toPojoResult(final ObjectManager om, Class<T> cls, Iterable<Entity> queryResultIterable, Cursor endCursor) {
    final ClassLoaderResolver clr = om.getClassLoaderResolver();
    final AbstractClassMetaData acmd = om.getMetaDataManager().getMetaDataForClass(cls, clr);
    Utils.Function<Entity, Object> func = new Utils.Function<Entity, Object>() {
      public Object apply(Entity from) {
        return DatastoreQuery.entityToPojo(from, acmd, clr, om, true, om.getFetchPlan().getCopy());
      }
    };
    AbstractJavaQuery query = new DummyQuery(om);
    DatastoreManager dm = (DatastoreManager) om.getStoreManager();
    ManagedConnection mconn = om.getStoreManager().getConnection(om);
    try {
      return (List<T>) DatastoreQuery.newStreamingQueryResultForEntities(
          queryResultIterable, func, mconn, endCursor, query, dm.isJPA());
    } finally {
      mconn.release();
    }
  }

  private static final class DummyQuery extends AbstractJavaQuery {

    private DummyQuery(ObjectManager om) {
      super(om);
    }

    public String getSingleStringQuery() {
      throw new UnsupportedOperationException();
    }

    protected void compileInternal(boolean forExecute, Map parameterValues) {
      throw new UnsupportedOperationException();
    }

    protected Object performExecute(Map parameters) {
      throw new UnsupportedOperationException();
    }
  }
}