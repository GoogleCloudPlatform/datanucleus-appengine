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

  @Override
  protected void clearInternal(StateManager ownerSM, ObjectManager om) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean updateElementFkInternal(StateManager sm, Object element, Object owner) {
    // Keys (and therefore parents) are immutable so we don't need to ever
    // actually update the parent FK, but we do need to check to make sure
    // someone isn't trying to modify the parent FK
    DatastoreRelationFieldManager.checkForParentSwitch(element, sm);
    // fk is already set and sets are unindexed so there's nothing else to do
    return true;
  }
}
