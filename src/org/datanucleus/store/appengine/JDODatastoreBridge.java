/*
 * Copyright (C) 2009 Max Ross.
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
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultList;

import org.datanucleus.ObjectManager;
import org.datanucleus.jdo.JDOPersistenceManager;

import java.util.List;

import javax.jdo.PersistenceManager;

/**
 * Utilities for converting between the low-level datastore api and JDO.
 * <br>
 * This class is part of the public api of the DataNucleus App Engine plugin
 * and can be safely used.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public final class JDODatastoreBridge extends PojoDatastoreBridge {

  /**
   * Convert the result of a low-level datastore query into a List of JDO
   * entities.
   *
   * @param pm The PersistenceManager with which the conversion results will be
   * associated.
   * @param cls The type of object to which the {@link Entity Entities} in the
   * result list should be converted.
   * @param queryResultList The result of the low-level datastore query.
   * @return The {@link Entity Entities} in the provided result list, converted
   * to JDO entities.
   *
   * @see PreparedQuery#asQueryResultList(FetchOptions)
   */
  public <T> List<T> toJDOResult(PersistenceManager pm, Class<T> cls, QueryResultList<Entity> queryResultList) {
    return toJDOResult(pm, cls, queryResultList, queryResultList.getCursor());
  }

  /**
   * Convert the result of a low-level datastore query into a List of JDO
   * entities.
   *
   * @param pm The PersistenceManager with which the conversion results will be
   * associated.
   * @param cls The type of object to which the {@link Entity Entities} in the
   * result iterable should be converted.
   * @param queryResultIterable The result of the low-level datastore query.
   * @return The {@link Entity Entities} in the provided result iterable,
   * converted to JDO entities.
   *
   * @see PreparedQuery#asQueryResultIterable()
   */
  public <T> List<T> toJDOResult(PersistenceManager pm, Class<T> cls, QueryResultIterable<Entity> queryResultIterable) {
    return toJDOResult(pm, cls, queryResultIterable, null);
  }

  private <T> List<T> toJDOResult(PersistenceManager pm, Class<T> cls, Iterable<Entity> queryResultIterable, Cursor endCursor) {
    ObjectManager om = ((JDOPersistenceManager) pm).getObjectManager();
    return toPojoResult(om, cls, queryResultIterable, endCursor);
  }
}
