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
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
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

  protected void clearInternal(ObjectProvider ownerOP, ExecutionContext ec) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean updateElementFkInternal(ObjectProvider op, Object element, Object owner) {
    // Keys (and therefore parents) are immutable so we don't need to ever
    // actually update the parent FK, but we do need to check to make sure
    // someone isn't trying to modify the parent FK
    DatastoreRelationFieldManager.checkForParentSwitch(element, op);
    // fk is already set and sets are unindexed so there's nothing else to do
    return true;
  }

  public Iterator iterator(ObjectProvider ownerOP) {
    ExecutionContext ec = ownerOP.getExecutionContext();
    ApiAdapter apiAdapter = ec.getApiAdapter();
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, ownerOP);
    return ((DatastoreAbstractSetStoreSpecialization) specialization).getChildren(
        parentKey,
        Collections.<Query.FilterPredicate>emptyList(),
        Collections.<Query.SortPredicate>emptyList(), this, ec).iterator();
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
  public boolean remove(ObjectProvider ownerOP, Object element, int size, boolean allowDependentField) {
    boolean result = super.remove(ownerOP, element, size,allowDependentField);
    if (result) {
      return result;
    }
    if (element == null) {
      return false;
    }
    if (!validateElementForReading(ownerOP, element)) {
      return false;
    }

    ExecutionContext ec = ownerOP.getExecutionContext();
    ObjectProvider elementOP = ec.findObjectProvider(element);
    if (ec.getApiAdapter().isDetached(element)) // User passed in detached object to collection.remove()!
    {
      // Find an attached equivalent of this detached object (DON'T attach the object itself)
      element = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false,
                        element.getClass().getName());
    }
    if (ec.getApiAdapter().isPersistable(element) && ec.getApiAdapter().isDeleted(element)) {
      // Element is waiting to be deleted so flush it (it has the FK)
      elementOP.flush();
    } else {
      // Element not yet marked for deletion so go through the normal process
      ec.deleteObjectInternal(element);
    }
    return true;
  }

  /**
   * For an FKSet (owned), removing the child should always delete the child.
   * This is necessary because there's no way to "null out" the parent key on child
   * because keys are immutable.
   */
  @Override
  protected boolean checkRemovalOfElementShouldDelete(ObjectProvider ownerOP) {
    return true;
  }
}
