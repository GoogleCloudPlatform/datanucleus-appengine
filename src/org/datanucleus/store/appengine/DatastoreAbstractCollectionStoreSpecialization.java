// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
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

  public boolean updateEmbeddedElement(StateManager sm, Object element, int fieldNumber,
      Object value, JavaTypeMapping fieldMapping, ElementContainerStore ecs) {
    // TODO(maxr)
    throw new UnsupportedOperationException();
  }

  public boolean contains(StateManager ownerSM, Object element, AbstractCollectionStore acs) {
    // Since we only support owned relationships right now, we can
    // check containment simply by looking to see if the element's
    // Key contains the parnet Key.
    ObjectManager om = ownerSM.getObjectManager();
    Key childKey = extractElementKey(om, element);
    if (childKey.getParent() == null) {
      return false;
    }
    Key parentKey = extractElementKey(om, ownerSM.getObject());
    return childKey.getParent().equals(parentKey);
  }

  public int[] internalRemove(StateManager ownerSM, ManagedConnection conn, boolean batched,
                              Object element, boolean executeNow, AbstractCollectionStore acs)
  throws MappedDatastoreException {
    // TODO(maxr) Only used by Map key and value stores, which we do not yet
    // support.
    throw new UnsupportedOperationException();
  }
}
