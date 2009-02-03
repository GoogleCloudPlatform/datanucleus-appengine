// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.store.mapped.scostore.FKArrayStoreSpecialization;
import org.datanucleus.util.Localiser;

/**
 * Datastore-specific implementation of {@link FKArrayStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFKArrayStoreSpecialization extends DatastoreAbstractArrayStoreSpecialization
    implements FKArrayStoreSpecialization {

  public DatastoreFKArrayStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
      DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public boolean getUpdateElementFk(StateManager sm, Object element, Object owner, int index,
      ElementContainerStore ecs) {
    JavaTypeMapping orderMapping = ecs.getOrderMapping();
    if (orderMapping != null) {
      DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
      ObjectManager om = sm.getObjectManager();
      StateManager childSm = om.findStateManager(element);
      Entity childEntity = handler.getAssociatedEntityForCurrentTransaction(childSm);
      orderMapping.setObject(sm.getObjectManager(), childEntity, new int[1], index);
      handler.put(om, childEntity);
      return true;
    }
    return false;
  }
}
