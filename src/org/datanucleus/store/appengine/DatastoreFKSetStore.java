// Copyright 2008 Google Inc. All Rightss Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.scostore.FKSetStore;

/**
 * Datastore-specific implementation of an {@link FKSetStore}.
 */
public class DatastoreFKSetStore extends FKSetStore {

  public DatastoreFKSetStore(AbstractMemberMetaData fmd, DatastoreManager storeMgr,
      ClassLoaderResolver clr) {
    super(fmd, storeMgr, clr, new DatastoreAbstractSetStoreSpecialization(LOCALISER, clr, storeMgr));
  }

  protected void clearInternal(StateManager ownerSM, ObjectManager om) {
    throw new UnsupportedOperationException();
  }

  protected boolean updateElementFkInternal(StateManager sm, Object element, Object owner) {
    // The fk is already set and sets are unindexed so there's nothing to do
    return true;
  }
}
