// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
class DatastoreRelationFieldManager {

  // Needed for relation management in datanucleus.
  private static final int[] NOT_USED = {0};

  private final DatastoreFieldManager fieldManager;

  // Events that know how to store relations.
  private final List<StoreRelationEvent> storeRelationEvents = Utils.newArrayList();

  DatastoreRelationFieldManager(DatastoreFieldManager fieldManager) {
    this.fieldManager = fieldManager;
  }

  /**
   * Applies all the relation events that have been built up.
   *
   * @return {@code true} if any of the events modified the parent
   * in such a way that the parent needs to be repersisted, {@link false}
   * otherwise.
   */
  boolean storeRelations(KeyRegistry keyRegistry) {
    if (storeRelationEvents.isEmpty()) {
      return false;
    }
    if (fieldManager.getEntity().getKey() != null) {
      keyRegistry.registerKey(getStoreManager(), getStateManager(), fieldManager);
    }
    for (StoreRelationEvent event : storeRelationEvents) {
      event.apply();
    }
    storeRelationEvents.clear();
    // TODO(maxr) Detect changes that modify the parent.
    return true;
  }

  private MappedStoreManager getStoreManager() {
    return fieldManager.getStoreManager();
  }

  void storeRelationField(final ClassLoaderResolver clr, final AbstractClassMetaData acmd,
                          final AbstractMemberMetaData ammd, final Object value,
                          final boolean isInsert) {
    StoreRelationEvent event = new StoreRelationEvent() {
      public void apply() {
        DatastoreClass dc = getStoreManager().getDatastoreClass(
            ammd.getAbstractClassMetaData().getFullClassName(),
            fieldManager.getClassLoaderResolver());

        StateManager sm = getStateManager();
        int fieldNumber = ammd.getAbsoluteFieldNumber();
        // Based on ParameterSetter
        try {
          JavaTypeMapping mapping = dc.getMemberMappingInDatastoreClass(ammd);
          if (mapping instanceof EmbeddedPCMapping ||
              mapping instanceof SerialisedPCMapping ||
              mapping instanceof SerialisedReferenceMapping ||
              mapping instanceof PersistenceCapableMapping ||
              mapping instanceof InterfaceMapping) {
            mapping.setObject(
                fieldManager.getObjectManager(),
                fieldManager.getEntity(),
                NOT_USED,
                value,
                sm,
                fieldNumber);
            // Make sure the field is wrapped where appropriate
            sm.wrapSCOField(fieldNumber, value, false, true, true);
          } else {
            if (isInsert) {
              runPostInsertMappingCallbacks(dc, clr, acmd);
            } else {
              runPostUpdateMappingCallbacks(dc, clr, acmd);
            }
          }
        } catch (NotYetFlushedException e) {
          if (acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getNullValue() == NullValue.EXCEPTION) {
            throw e;
          }
          sm.updateFieldAfterInsert(e.getPersistable(), fieldNumber);
        }
      }
    };

    // If the related object can't exist without the parent it should be part
    // of the parent's entity group.  In order to be part of the parent's
    // entity group we need the parent's key.  In order to get the parent's key
    // we must save the parent before we save the child.  In order to avoid
    // saving the child until after we've saved the parent we register an event
    // that we will apply later.  Note that for one-to-many we need to apply
    // the event after the parent has been saved whether the relationship is
    // dependent or not because we need the parent key in the child table.
    // TODO(maxr) Support storing child keys in the parent table as a list
    // property.
    if (ammd.isDependent() || ammd.getRelationType(clr) == Relation.ONE_TO_MANY_BI ||
        ammd.getRelationType(clr) == Relation.ONE_TO_MANY_UNI) {
      storeRelationEvents.add(event);
    } else {
      // not dependent so just apply right away
      event.apply();
    }
  }

  private DependentInsertMappingConsumer buildMappingConsumerForWrite(DatastoreClass dc,
                                                                      ClassLoaderResolver clr,
                                                                      AbstractClassMetaData acmd) {
    DependentInsertMappingConsumer consumer = new DependentInsertMappingConsumer(clr, acmd);
    dc.provideDatastoreIdMappings(consumer);
    dc.provideNonPrimaryKeyMappings(consumer);
    dc.providePrimaryKeyMappings(consumer);
    return consumer;
  }

  private void runPostInsertMappingCallbacks(DatastoreClass dc, ClassLoaderResolver clr,
                                             AbstractClassMetaData acmd) {
    DependentInsertMappingConsumer consumer = buildMappingConsumerForWrite(dc, clr, acmd);
    StateManager sm = getStateManager();
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      callback.postInsert(sm);
    }
  }

  private void runPostUpdateMappingCallbacks(DatastoreClass dc, ClassLoaderResolver clr,
                                             AbstractClassMetaData acmd) {
    DependentInsertMappingConsumer consumer = buildMappingConsumerForWrite(dc, clr, acmd);
    StateManager sm = getStateManager();
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      callback.postUpdate(sm);
    }
  }

  /**
   * If the datastore entity doesn't have a parent and some other
   * pojo in the cascade chain registered itself as the parent for
   * this pojo, recreate the datastore entity with the parent pojo's
   * key as the ancestor.
   *
   * @param keyRegistry the key registry
   * @return The parent key if the pojo class has an ancestor property.
   */
  public Object establishEntityGroup(KeyRegistry keyRegistry) {
    Key parentKey = keyRegistry.getRegisteredKey(getStateManager().getObject());
    if (parentKey != null && fieldManager.getEntity().getParent() == null) {
      fieldManager.recreateEntityWithAncestor(KeyFactory.encodeKey(parentKey));
      if (getAncestorMemberMetaData() != null) {
        return getAncestorMemberMetaData().getType().equals(Key.class)
            ? parentKey : KeyFactory.encodeKey(parentKey);
      }
    }
    return null;
  }

  Object fetchRelationField(ClassLoaderResolver clr, AbstractClassMetaData acmd,
                            AbstractMemberMetaData ammd) {
    DatastoreClass dc = getStoreManager().getDatastoreClass(
        ammd.getAbstractClassMetaData().getFullClassName(), fieldManager.getClassLoaderResolver());
    JavaTypeMapping mapping = dc.getMemberMappingInDatastoreClass(ammd);
    // Based on ResultSetGetter
    Object value;
    if (mapping instanceof EmbeddedPCMapping ||
        mapping instanceof SerialisedPCMapping ||
        mapping instanceof SerialisedReferenceMapping) {
      value = mapping.getObject(
          fieldManager.getObjectManager(),
          fieldManager.getEntity(),
          NOT_USED,
          getStateManager(),
          ammd.getAbsoluteFieldNumber());
    } else {
      int relationType = ammd.getRelationType(clr);
      if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) {
        // Extract the related key from the entity
        String propName = EntityUtils.getPropertyName(fieldManager.getIdentifierFactory(), ammd);
        Key relatedKey = (Key) fieldManager.getEntity().getProperty(propName);
        value = mapping.getObject(fieldManager.getObjectManager(), relatedKey, NOT_USED);
      } else if (relationType == Relation.MANY_TO_ONE_BI) {
        // Extract the parent key from the entity
        Key parentKey = fieldManager.getEntity().getParent();
        value = mapping.getObject(fieldManager.getObjectManager(), parentKey, NOT_USED);
      } else {
        value = null;
      }
    }
    // Return the field value (as a wrapper if wrappable)
    return getStateManager().wrapSCOField(ammd.getAbsoluteFieldNumber(), value, false, false, false);
  }

  private AbstractMemberMetaData getAncestorMemberMetaData() {
    return fieldManager.getAncestorMemberMetaData();
  }

  private StateManager getStateManager() {
    return fieldManager.getStateManager();
  }
  /**
   * Supports a mechanism for delaying the storage of a relation.
   */
  private interface StoreRelationEvent {
    void apply();
  }
}
