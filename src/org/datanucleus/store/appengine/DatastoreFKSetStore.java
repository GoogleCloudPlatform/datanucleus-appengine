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
