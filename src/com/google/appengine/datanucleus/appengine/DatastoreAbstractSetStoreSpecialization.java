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
import org.datanucleus.ManagedConnection;
import org.datanucleus.StateManager;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.scostore.AbstractSetStore;
import org.datanucleus.store.mapped.scostore.AbstractSetStoreSpecialization;
import org.datanucleus.util.Localiser;

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

  public int[] internalAdd(StateManager sm, ManagedConnection mconn, boolean batched,
      Object element, boolean processNow, AbstractSetStore abstractSetStore)
      throws MappedDatastoreException {
    // TODO(maxr) Only invoked from AbstractSetStore.addAll and add,
    // but both of these are overridden in FKSetStore, which is the only
    // Set store we currently support.
    throw new UnsupportedOperationException();
  }

  public boolean remove(StateManager sm, Object element, int size, AbstractSetStore setStore) {
    // TODO(maxr) Only invoked from AbstractSetStore.remove, but this is
    // overridden in FKSetStore, which is the only Set store we currently support.
    throw new UnsupportedOperationException();
  }

  public void preInternalRemove(ManagedConnection mconn) throws MappedDatastoreException {
    // TODO(maxr) Only invoked from AbstractSetStore.removeAll, but this is
    // overridden in FKSetStore, which is the only Set store we currently support.
    throw new UnsupportedOperationException();
  }
}