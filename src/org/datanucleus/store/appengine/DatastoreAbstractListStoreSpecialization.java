// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.AbstractListStore;
import org.datanucleus.store.mapped.scostore.AbstractListStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.util.Localiser;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * Datastore-specific implementation of {@link AbstractListStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
abstract class DatastoreAbstractListStoreSpecialization extends DatastoreAbstractCollectionStoreSpecialization
  implements AbstractListStoreSpecialization {

  DatastoreAbstractListStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
                                           DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public int indexOf(StateManager sm, Object element, ElementContainerStore ecs) {
    // TODO(maxr) Figure out in what context this gets called
    throw new UnsupportedOperationException();
  }

  public int lastIndexOf(StateManager sm, Object element, ElementContainerStore ecs) {
    // TODO(maxr) Figure out in what context this gets called
    throw new UnsupportedOperationException();
  }

  public int[] getIndicesOf(StateManager sm, Collection elements, ElementContainerStore ecs) {
    // TODO(maxr) Figure out in what context this gets called
    throw new UnsupportedOperationException();
  }

  public ListIterator listIterator(QueryExpression stmt, boolean useUpdateLock, ObjectManager om,
      StateManager ownerSM, ResultObjectFactory rof,
      AbstractListStore als) {
    return listIterator(stmt, om, ownerSM, als);
  }

  public List internalGetRange(ObjectManager om, boolean useUpdateLock, QueryExpression stmt,
                               ResultObjectFactory getROF, ElementContainerStore ecs) {
    DatastoreQueryExpression dqe = (DatastoreQueryExpression) stmt;
    Key parentKey = dqe.getParentKey();
    if (parentKey == null) {
      throw new UnsupportedOperationException("Could not extract parent key from query expression.");
    }
    return getChildren(parentKey, dqe.getFilterPredicates(), dqe.getSortPredicates(), ecs, om);
  }

  public int[] internalShift(StateManager ownerSM, ManagedConnection conn, boolean batched,
      int oldIndex, int amount, boolean executeNow,
      ElementContainerStore ecs) throws MappedDatastoreException {
    JavaTypeMapping orderMapping = ecs.getOrderMapping();
    if (orderMapping == null) {
      return null;
    }
    DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService();
    AbstractClassMetaData acmd = ecs.getEmd();
    String kind =
        storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
    Query q = new Query(kind);
    ObjectManager om = ownerSM.getObjectManager();
    Object id = om.getApiAdapter().getTargetKeyForSingleFieldIdentity(
        ownerSM.getInternalObjectId());
    Key key = id instanceof Key ? (Key) id : KeyFactory.stringToKey((String) id);
    q.setAncestor(key);
    // create an entity just to capture the name of the index property
    Entity entity = new Entity(kind);
    orderMapping.setObject(om, entity, new int[] {1}, oldIndex);
    String indexProp = entity.getProperties().keySet().iterator().next();
    q.addFilter(indexProp, Query.FilterOperator.GREATER_THAN_OR_EQUAL, oldIndex);
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    for (Entity shiftMe : service.prepare(q).asIterable()) {
      Long pos = (Long) shiftMe.getProperty(indexProp);
      shiftMe.setProperty(indexProp, pos + amount);
      handler.put(om, shiftMe);
    }
    return null;
  }
}
