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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
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

  private final ThreadLocal<Boolean> removing = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  DatastoreFKListStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
      DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  /**
   * This is invoked when someone sets an element at a specific location
   * in a list.  We need to update the child entity with the magical
   * index property and, if there was a dependent object at the
   * location in the list that is being written to, delete the dependent
   * object.
   */
  public Object set(StateManager ownerSm, int index, Object element, boolean allowDependentField,
      ElementContainerStore ecs, Object obj) {
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    JavaTypeMapping orderMapping = ecs.getOrderMapping();
    ObjectManager om = ownerSm.getObjectManager();
    if (orderMapping != null) {
      StateManager childSm = om.findStateManager(element);
      // See DatastoreFieldManager.handleIndexFields for info on why this
      // absurdity is necessary.
      Entity childEntity =
          (Entity) childSm.getAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED);
      if (childEntity != null) {
        childSm.setAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED, null);
        childSm.setAssociatedValue(ecs.getOrderMapping(), index);
        handler.insertObject(childSm);
      }
    }

    if (ecs.getOwnerMemberMetaData().getCollection().isDependentElement() &&
        allowDependentField && obj != null) {
      ownerSm.getObjectManager().deleteObjectInternal(obj);
    }
    if (!om.getTransaction().isActive()) {
      om.getTransaction().addTransactionEventListener(
          new ForceFlushPreCommitTransactionEventListener(ownerSm));
    }
    return obj;
  }

  public boolean updateElementFk(StateManager sm, Object element, Object parent, int index,
      ObjectManager om, ElementContainerStore ecs) {
    // Keys (and therefore parents) are immutable so we don't need to ever
    // actually update the parent FK, but we do need to check to make sure
    // someone isn't trying to modify the parent FK
    DatastoreRelationFieldManager.checkForParentSwitch(element, sm);

    if (ecs.getOrderMapping() == null) {
      return false;
    }
    StateManager elementSm = om.findStateManager(element);
    // The fk is already set but we still need to set the index
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    // See DatastoreFieldManager.handleIndexFields for info on why this
    // absurdity is necessary.
    Entity entity =
        (Entity) elementSm.getAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED);
    if (entity != null) {
      elementSm.setAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED, null);
      elementSm.setAssociatedValue(ecs.getOrderMapping(), index);
      if (entity.getParent() == null) {
        StateManager parentSm = om.findStateManager(parent);
        // need to register the proper parent for this entity
        Key parentKey = EntityUtils.getPrimaryKeyAsKey(om.getApiAdapter(), parentSm);
        KeyRegistry.getKeyRegistry(om).registerKey(element, parentKey, ecs.getElementType());
      }
      handler.insertObject(elementSm);
    }
    return true;
  }

  public void clearWithoutDelete(ObjectManager om, StateManager ownerSM,
      ElementContainerStore ecs) {
    throw new UnsupportedOperationException("Non-owned relationships are not currently supported");
  }

  public void removeAt(StateManager sm, int index, int size, boolean nullify,
      FKListStore fkListStore) {
    if (removing.get()) {
      return;
    }
    if (nullify) {
      // we don't support unowned relationships yet
      throw new UnsupportedOperationException(
          "Non-owned relationships are not currently supported.");
    } else {
      // first we need to delete the element
      ObjectManager om = sm.getObjectManager();
      Object element = fkListStore.get(sm, index);
      StateManager elementStateManager = om.findStateManager(element);
      DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
      // the delete call can end up cascading back here, so set a thread-local
      // to make sure we don't do it more than once
      removing.set(true);
      try {
        handler.deleteObject(elementStateManager);
      } finally {
        removing.set(false);
      }
      if (fkListStore.getOrderMapping() != null) {
        // now, if there is an order mapping, we need to shift
        // everyone down
        JavaTypeMapping orderMapping = fkListStore.getOrderMapping();
        DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfig();
        DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
        AbstractClassMetaData acmd = fkListStore.getEmd();
        String kind =
            storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
        Query q = new Query(kind);
        Key key = EntityUtils.getPrimaryKeyAsKey(sm.getObjectManager().getApiAdapter(), sm);
        q.setAncestor(key);
        // create an entity just to capture the name of the index property
        Entity entity = new Entity(kind);
        orderMapping.setObject(om, entity, new int[] {1}, index);
        String indexProp = entity.getProperties().keySet().iterator().next();
        q.addFilter(indexProp, Query.FilterOperator.GREATER_THAN, index);
        for (Entity shiftMe : service.prepare(service.getCurrentTransaction(null), q).asIterable()) {
          Long pos = (Long) shiftMe.getProperty(indexProp);
          shiftMe.setProperty(indexProp, pos - 1);
          handler.put(om, shiftMe);
        }
      }
    }
  }
}
