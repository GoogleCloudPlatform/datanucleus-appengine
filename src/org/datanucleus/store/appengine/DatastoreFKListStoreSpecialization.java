// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.store.mapped.scostore.FKListStore;
import org.datanucleus.store.mapped.scostore.FKListStoreSpecialization;
import org.datanucleus.util.Localiser;

/**
 * Datastore-specific implementation of {@link FKListStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreFKListStoreSpecialization extends DatastoreAbstractListStoreSpecialization
    implements FKListStoreSpecialization {

  DatastoreFKListStoreSpecialization(Localiser localiser, ClassLoaderResolver clr, DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public Object set(StateManager sm, int index, Object element, boolean allowDependentField,
                    ElementContainerStore ecs, Object o) {
    // TODO(maxr)
    return null;
  }

  public boolean updateElementFk(StateManager sm, Object element, Object owner, int index,
                                 ObjectManager om, ElementContainerStore ecs) {
    // TODO(maxr)
    return false;
  }

  public void clearWithoutDelete(ObjectManager om, StateManager ownerSM,
                                 ElementContainerStore ecs) {
    // TODO(maxr)
  }

  public void removeAt(StateManager sm, int index, int size, boolean nullify,
                       FKListStore fkListStore) {
    // TODO(maxr)
  }

  public JavaTypeMapping getUnidirectionalOwnerMapping(
      AbstractMemberMetaData ammd, FKListStore fkListStore) {
    return fkListStore.getElementInfo()[0].getDatastoreClass()
        .getExternalMapping(ammd, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
  }
}
