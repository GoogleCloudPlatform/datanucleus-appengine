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
package com.google.appengine.datanucleus.scostore;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.EntityUtils;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.AbstractCollectionStore;
import org.datanucleus.store.mapped.scostore.AbstractCollectionStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.util.Localiser;

/**
 * Datastore-specific implementation of
 * {@link AbstractCollectionStoreSpecialization}.
 * 
 * @author Max Ross <maxr@google.com>
 */
abstract class DatastoreAbstractCollectionStoreSpecialization
    extends DatastoreElementContainerStoreSpecialization
    implements AbstractCollectionStoreSpecialization {

  DatastoreAbstractCollectionStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
                                                 DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public boolean updateEmbeddedElement(ObjectProvider op, Object element, int fieldNumber,
      Object value, JavaTypeMapping fieldMapping, ElementContainerStore ecs) {
    // TODO(maxr)
    throw new UnsupportedOperationException();
  }

  public boolean contains(ObjectProvider ownerOP, Object element, AbstractCollectionStore acs) {
    // Since we only support owned relationships right now, we can
    // check containment simply by looking to see if the element's
    // Key contains the parent Key.
    ExecutionContext ec = ownerOP.getExecutionContext();
    Key childKey = extractElementKey(ec, element);
    // Child key can be null if element has not yet been persisted
    if (childKey == null || childKey.getParent() == null) {
      return false;
    }
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), ownerOP);
    return childKey.getParent().equals(parentKey);
  }

  public int[] internalRemove(ObjectProvider ownerOP, ManagedConnection conn, boolean batched,
                              Object element, boolean executeNow, AbstractCollectionStore acs)
  throws MappedDatastoreException {
    // TODO(maxr) Only used by Map key and value stores, which we do not yet support.
    throw new UnsupportedOperationException();
  }
}
