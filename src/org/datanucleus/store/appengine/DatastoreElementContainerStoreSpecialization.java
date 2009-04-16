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
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.query.DiscriminatorIteratorStatement;
import org.datanucleus.store.mapped.query.UnionIteratorStatement;
import org.datanucleus.store.mapped.scostore.BaseElementContainerStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

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
      ElementContainerStore ecs) {
    String kind = storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(
        ecs.getEmd()).getIdentifierName();
    Query q = new Query(kind, parentKey);
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
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService();
    return ds.prepare(q);
  }

  List<?> getChildren(Key parentKey, Iterable<FilterPredicate> filterPredicates,
      Iterable<SortPredicate> sortPredicates, ElementContainerStore ecs, ObjectManager om) {
    List<Object> result = new ArrayList<Object>();
    int numChildren = 0;
    for (Entity e : prepareChildrenQuery(parentKey, filterPredicates, sortPredicates, ecs).asIterable()) {
      // We only want direct children
      if (parentKey.equals(e.getKey().getParent())) {
        numChildren++;
        result.add(DatastoreQuery.entityToPojo(e, ecs.getEmd(), clr, storeMgr, om, false));
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
        Collections.<SortPredicate>emptyList(),
        ecs).asIterable();
    int count = 0;
    for (Entity e : children) {
      if (parentKey.equals(e.getKey().getParent())) {
        count++;
      }
    }
    return count;
  }

  public DiscriminatorIteratorStatement newDiscriminatorIteratorStatement(ClassLoaderResolver clr,
      Class[] cls, boolean includeSubclasses, boolean selectDiscriminator) {
    // TODO(maxr)
    throw new UnsupportedOperationException("Discriminators not supported.");
  }

  public DiscriminatorIteratorStatement newDiscriminatorIteratorStatement(ClassLoaderResolver clr,
      Class[] cls, boolean b, boolean b1, boolean allowsNull,
      DatastoreContainerObject containerTable, JavaTypeMapping elementMapping,
      DatastoreIdentifier elmIdentifier) {
    // TODO(maxr)
    throw new UnsupportedOperationException("Discriminators not supported.");
  }

  public UnionIteratorStatement newUnionIteratorStatement(ClassLoaderResolver clr,
      Class candidateType, boolean includeSubclasses, Class sourceType,
      JavaTypeMapping sourceMapping, DatastoreContainerObject sourceTable, boolean sourceJoin,
      Boolean withMetadata, boolean joinToExcludeTargetSubclasses, boolean allowsNull) {
    return new DatastoreUnionIteratorStatement(clr, candidateType, includeSubclasses, storeMgr,
                                               sourceType, sourceMapping, sourceTable, sourceJoin,
                                               withMetadata, joinToExcludeTargetSubclasses,
                                               allowsNull);
  }

  ListIterator listIterator(
      QueryExpression stmt, ObjectManager om,StateManager ownerSM, ElementContainerStore ecs) {
    // for now we're just going to perform a kind + ancestor query using the owning
    // object
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    Entity parentEntity = handler.getAssociatedEntityForCurrentTransaction(ownerSM);
    if (parentEntity == null) {
      handler.locateObject(ownerSM);
      parentEntity = handler.getAssociatedEntityForCurrentTransaction(ownerSM);
    }
    DatastoreQueryExpression dqe = (DatastoreQueryExpression) stmt;
    return getChildren(
        parentEntity.getKey(),
        dqe.getFilterPredicates(),
        dqe.getSortPredicates(),
        ecs, om).listIterator();
  }

  protected Key extractElementKey(ObjectManager om, Object element) {
    ApiAdapter apiAdapter = om.getApiAdapter();
    Object id = apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(element));
    return id instanceof Key ? (Key) id : KeyFactory.stringToKey((String) id);
  }
}
