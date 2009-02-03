// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.scostore.AbstractSetStore;
import org.datanucleus.store.mapped.scostore.AbstractSetStoreSpecialization;
import org.datanucleus.store.query.ResultObjectFactory;
import org.datanucleus.util.Localiser;

import java.util.Iterator;

/**
 * Datastore-specific implementation of {@link AbstractSetStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreAbstractSetStoreSpecialization extends DatastoreAbstractCollectionStoreSpecialization
  implements AbstractSetStoreSpecialization {

  DatastoreAbstractSetStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
      DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public Iterator iterator(QueryExpression stmt, boolean useUpdateLock, ObjectManager om,
      StateManager ownerSM, ResultObjectFactory rof, AbstractSetStore setStore) {
    DatastoreQueryExpression dqe = (DatastoreQueryExpression) stmt;
    Key parentKey = dqe.getParentKey();
    if (parentKey == null) {
      throw new UnsupportedOperationException("Could not extract parent key from query expression.");
    }
    return getChildren(
        parentKey, dqe.getFilterPredicates(), dqe.getSortPredicates(), setStore, om).iterator();
  }

  public int[] internalAdd(StateManager sm, ManagedConnection mconn, boolean batched,
      Object element, boolean processNow, AbstractSetStore abstractSetStore)
      throws MappedDatastoreException {
    throw new UnsupportedOperationException();
  }

  public boolean remove(StateManager sm, Object element, int size, AbstractSetStore setStore) {
    throw new UnsupportedOperationException();
  }

  public void preInternalRemove(ManagedConnection mconn) throws MappedDatastoreException {
    throw new UnsupportedOperationException();
  }
}