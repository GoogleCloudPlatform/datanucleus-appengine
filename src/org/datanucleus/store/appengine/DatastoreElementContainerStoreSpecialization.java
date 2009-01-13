// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.PreparedQuery;
import com.google.apphosting.api.datastore.Query;
import com.google.apphosting.api.datastore.Query.SortPredicate;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.store.appengine.query.DatastoreQuery;
import org.datanucleus.store.mapped.scostore.BaseElementContainerStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.util.Localiser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Datastore-specific extension to
 * {@link BaseElementContainerStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreElementContainerStoreSpecialization extends BaseElementContainerStoreSpecialization {

  protected final DatastoreManager storeMgr;

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

  private PreparedQuery prepareChildrenQuery(Key parentKey, Iterable<SortPredicate> sortPredicates,
                                             ElementContainerStore ecs) {
    String kind = storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(
        ecs.getEmd()).getIdentifierName();
    Query q = new Query(kind, parentKey);
    for (SortPredicate sp : sortPredicates) {
      q.addSort(sp.getPropertyName(), sp.getDirection());
    }
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService();
    return ds.prepare(q);
  }

  List<?> getChildren(Key parentKey, Iterable<SortPredicate> sortPredicates,
                      ElementContainerStore ecs, ObjectManager om) {
    List<Object> result = new ArrayList<Object>();
    for (Entity e : prepareChildrenQuery(parentKey, sortPredicates, ecs).asIterable()) {
      result.add(DatastoreQuery.entityToPojo(e, ecs.getEmd(), clr, storeMgr, om, false));
    }
    return result;
  }

  int getNumChildren(Key parentKey, ElementContainerStore ecs) {
    return prepareChildrenQuery(
        parentKey, Collections.<SortPredicate>emptyList(), ecs).countEntities();
  }
}
