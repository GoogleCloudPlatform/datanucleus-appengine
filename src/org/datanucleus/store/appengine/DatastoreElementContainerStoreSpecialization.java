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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortPredicate;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.store.appengine.query.DatastoreQuery;
import org.datanucleus.store.mapped.scostore.BaseElementContainerStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Datastore-specific extension to
 * {@link BaseElementContainerStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
abstract class DatastoreElementContainerStoreSpecialization extends BaseElementContainerStoreSpecialization {

  private static final NucleusLogger logger = NucleusLogger.DATASTORE_RETRIEVE;

  final DatastoreManager storeMgr;

  DatastoreElementContainerStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
                                               DatastoreManager storeMgr) {
    super(localiser, clr);
    this.storeMgr = storeMgr;
  }

  public void executeClear(StateManager ownerSM, ElementContainerStore elementContainerStore) {
    // TODO(maxr)
  }

  public int getSize(StateManager sm, ElementContainerStore ecs) {
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    Entity parentEntity = handler.getAssociatedEntityForCurrentTransaction(sm);
    if (parentEntity == null) {
      handler.locateObject(sm);
      parentEntity = handler.getAssociatedEntityForCurrentTransaction(sm);
    }
    return getNumChildren(parentEntity.getKey(), ecs);
  }

  PreparedQuery prepareChildrenQuery(
      Key parentKey,
      Iterable<FilterPredicate> filterPredicates,
      Iterable<SortPredicate> sortPredicates,
      boolean keysOnly,
      ElementContainerStore ecs) {
    String kind = storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(
        ecs.getEmd()).getIdentifierName();
    Query q = new Query(kind, parentKey);
    if (keysOnly) {
      q.setKeysOnly();
    }
    logger.debug("Preparing to query for all children of " + parentKey + " of kind " + kind);
    for (FilterPredicate fp : filterPredicates) {
      q.addFilter(fp.getPropertyName(), fp.getOperator(), fp.getValue());
      logger.debug(
          "  Added filter: " + fp.getPropertyName() + " " + fp.getOperator() + " " + fp.getValue());
    }
    for (SortPredicate sp : sortPredicates) {
      q.addSort(sp.getPropertyName(), sp.getDirection());
      logger.debug("  Added sort: " + sp.getPropertyName() + " " + sp.getDirection());
    }
    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);
    return ds.prepare(q);
  }

  List<?> getChildren(Key parentKey, Iterable<FilterPredicate> filterPredicates,
      Iterable<SortPredicate> sortPredicates, ElementContainerStore ecs, ObjectManager om) {
    List<Object> result = new ArrayList<Object>();
    int numChildren = 0;
    for (Entity e : prepareChildrenQuery(parentKey, filterPredicates, sortPredicates, false, ecs).asIterable()) {
      // We only want direct children
      if (parentKey.equals(e.getKey().getParent())) {
        numChildren++;
        result.add(DatastoreQuery.entityToPojo(e, ecs.getEmd(), clr, om, false, om.getFetchPlan()));
        if (logger.isDebugEnabled()) {
          logger.debug("Retrieved entity with key " + e.getKey());
        }
      }
    }
    logger.debug(String.format("Query had %d result%s.", numChildren, numChildren == 1 ? "" : "s"));
    return result;
  }

  int getNumChildren(Key parentKey, ElementContainerStore ecs) {
    Iterable<Entity> children = prepareChildrenQuery(
        parentKey,
        Collections.<FilterPredicate>emptyList(),
        // we're just counting so sort isn't important
        Collections.<SortPredicate>emptyList(),
        true,
        ecs).asIterable();
    int count = 0;
    for (Entity e : children) {
      if (parentKey.equals(e.getKey().getParent())) {
        count++;
      }
    }
    return count;
  }

  protected Key extractElementKey(ObjectManager om, Object element) {
    ApiAdapter apiAdapter = om.getApiAdapter();
    Object id = apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(element));
    if (id == null) {
      return null;
    }
    // This is a child object so we know the pk is Key or encoded String
    return id instanceof Key ? (Key) id : KeyFactory.stringToKey((String) id);
  }
}
