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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.scostore.FKSetStore;

import java.util.Collections;
import java.util.Iterator;

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

  public Iterator iterator(StateManager ownerSM) {
    ObjectManager om = ownerSM.getObjectManager();
    ApiAdapter apiAdapter = om.getApiAdapter();
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, ownerSM);
    return ((DatastoreAbstractSetStoreSpecialization) specialization).getChildren(
        parentKey,
        Collections.<Query.FilterPredicate>emptyList(),
        Collections.<Query.SortPredicate>emptyList(), this, om).iterator();
  }

  /**
   * More extreme misfortune.  When a child is removed from a bidirectional
   * one to many, DataNuc sets the parent field to null.  FKSetStore.remove()
   * sees that the parent has changed and declines to delete the child.
   * We want to force the delete of the child, so we have this fairly
   * nasty override that calls the super() implementation first.  If
   * this implementation declines to perform the delete (as evidenced by
   * a 'false' return value) we then duplicate _some_ of the logic to
   * try and force the delete to happen.  This is really brittle.
   */
  @Override
  public boolean remove(StateManager ownerSM, Object element, int size, boolean allowDependentField) {
    boolean result = super.remove(ownerSM, element, size,allowDependentField);
    if (result) {
      return result;
    }
    if (element == null) {
      return false;
    }
    if (!validateElementForReading(ownerSM, element)) {
      return false;
    }

    ObjectManager om = ownerSM.getObjectManager();
    StateManager elementSM = om.findStateManager(element);
    if (om.getApiAdapter().isDetached(element)) // User passed in detached object to collection.remove()!
    {
      // Find an attached equivalent of this detached object (DON'T attach the object itself)
      element = om.findObject(om.getApiAdapter().getIdForObject(element), true, false,
                        element.getClass().getName());
    }
    if (om.getApiAdapter().isPersistable(element) && om.getApiAdapter().isDeleted(element)) {
      // Element is waiting to be deleted so flush it (it has the FK)
      elementSM.flush();
    } else {
      // Element not yet marked for deletion so go through the normal process
      om.deleteObjectInternal(element);
    }
    return true;
  }

  /**
   * For an FKSet (owned), removing the child should always delete the child.
   * This is necessary because there's no way to "null out" the parent key on child
   * because keys are immutable.
   */
  @Override
  protected boolean checkRemovalOfElementShouldDelete(StateManager ownerSM) {
    return true;
  }
}
