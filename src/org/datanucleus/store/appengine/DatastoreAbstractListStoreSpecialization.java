// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.scostore.AbstractListStore;
import org.datanucleus.store.mapped.scostore.AbstractListStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.util.Localiser;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * Datastore-specific implementation of
 * {@link AbstractListStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreAbstractListStoreSpecialization extends DatastoreAbstractCollectionStoreSpecialization
  implements AbstractListStoreSpecialization {

  DatastoreAbstractListStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
                                           DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public int indexOf(StateManager sm, Object element, ElementContainerStore ecs) {
    // TODO(maxr)
    return 0;
  }

  public int lastIndexOf(StateManager sm, Object element, ElementContainerStore ecs) {
    // TODO(maxr)
    return 0;
  }

  public int[] getIndicesOf(StateManager sm, Collection elements, ElementContainerStore ecs) {
    // TODO(maxr)
    return null;
  }

  public ListIterator listIterator(QueryExpression stmt, boolean useUpdateLock, ObjectManager om,
                                   StateManager ownerSM, ResultObjectFactory rof,
                                   AbstractListStore als) {
    // for now we're just going to perform an ancstor query using the owning
    // object
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    Entity parentEntity = handler.getAssociatedEntityForCurrentTransaction(ownerSM);
    if (parentEntity == null) {
      handler.locateObject(ownerSM);
      parentEntity = handler.getAssociatedEntityForCurrentTransaction(ownerSM);
    }
    DatastoreQueryExpression dqe = (DatastoreQueryExpression) stmt;
    return getChildren(parentEntity.getKey(), dqe.getSortPredicates(), als, om).listIterator();
  }

  public List internalGetRange(ObjectManager om, boolean useUpdateLock, QueryExpression stmt,
                               ResultObjectFactory getROF, ElementContainerStore ecs) {
    DatastoreQueryExpression dqe = (DatastoreQueryExpression) stmt;
    Key parentKey = dqe.getParentKey();
    if (parentKey == null) {
      throw new UnsupportedOperationException("Could not extract parent key from query expression.");
    }
    return getChildren(parentKey, dqe.getSortPredicates(), ecs, om);
  }

  public int[] internalShift(StateManager ownerSM, ManagedConnection conn, boolean batched,
                               int oldIndex, int amount, boolean executeNow,
                               ElementContainerStore ecs) throws MappedDatastoreException {
    // TODO(maxr)
    return null;
  }
}
