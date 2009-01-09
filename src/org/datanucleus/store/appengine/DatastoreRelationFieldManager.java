// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Max Ross <maxr@google.com>
 */
class DatastoreRelationFieldManager {

  // Needed for relation management in datanucleus.
  private static final int[] NOT_USED = {0};

  // Thread local map is used to pass messages between parent and child
  // during cascades.  We're explicit about using an IdentityHashMap here
  // because we really really (really) want reference equality, not object
  // equality.  Field is not private to facilitate testing.
  static final ThreadLocal<IdentityHashMap<Object, Key>> PARENT_KEY_MAP =
      new ThreadLocal<IdentityHashMap<Object, Key>>() {
    @Override
    protected IdentityHashMap<Object, Key> initialValue() {
      return Maps.newIdentityHashMap();
    }
  };

  private final DatastoreFieldManager fieldManager;

  // Events that know how to store relations.
  private final List<StoreRelationEvent> storeRelationEvents = Lists.newArrayList();

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
  boolean storeRelations() {
    if (storeRelationEvents.isEmpty()) {
      return false;
    }
    Map<Object, Key> parentKeyMap = PARENT_KEY_MAP.get();
    Map<Object, Key> original = null;
    try {
      if (fieldManager.getEntity().getKey() != null) {
        original = makeKeyAvailableToRelatedObjects(parentKeyMap, original);
      }
      for (StoreRelationEvent event : storeRelationEvents) {
        event.apply();
      }
    } finally {
      if (original != null) {
        // This is an easy place to leak memory so make sure we restore
        // the map to its original state.  The map lives in a threadlocal
        // so we don't need to worry about concurrent access.
        parentKeyMap.clear();
        parentKeyMap.putAll(original);
      }
    }
    storeRelationEvents.clear();
    // TODO(maxr) Detect changes that modify the parent.
    return true;
  }

  /**
   * In order to automatically assign children to the same entity groups as
   * their parents, we need to make sure the Key of the parent entity is
   * available to the children when their corresponding entities are created.
   * We do this by looping over all the dependent fields of the parent, and for
   * each dependent field we extract the value of that field from the parent.
   * We then add that field value as the key in an indentity map, associating
   * it with the Key of the parent entity.  Anytime we insert a new object
   * we need to consult this map to see if a parent Key has been mapped to it,
   * and if it has we use that Key as the parent key for the new Entity we
   * are creating.
   */
  private Map<Object, Key> makeKeyAvailableToRelatedObjects(
      Map<Object, Key> parentKeyMap, Map<Object, Key> original) {
    DatastoreTable dt = (DatastoreTable) getStoreManager().getDatastoreClass(
        fieldManager.getClassMetaData().getFullClassName(), fieldManager.getClassLoaderResolver());
    SingleValueFieldManager sfv = new SingleValueFieldManager();
    for (AbstractMemberMetaData dependent : dt.getDependentMemberMetaData()) {
      getStateManager().provideFields(new int[]{dependent.getAbsoluteFieldNumber()}, sfv);
      Object dependentValue = sfv.fetchObjectField(dependent.getAbsoluteFieldNumber());
      if (dependentValue != null) {
        if (original == null) {
          // try to avoid making a copy of this map unless we really have to
          original = Maps.newHashMap(parentKeyMap);
        }
        PARENT_KEY_MAP.get().put(dependentValue, fieldManager.getEntity().getKey());
      }
    }
    return original;
  }

  private MappedStoreManager getStoreManager() {
    return fieldManager.getStoreManager();
  }

  void storeRelationField(final AbstractMemberMetaData ammd, final Object value) {
    StoreRelationEvent event = new StoreRelationEvent() {
      public void apply() {
        DatastoreClass dc = getStoreManager().getDatastoreClass(
            ammd.getAbstractClassMetaData().getFullClassName(),
            fieldManager.getClassLoaderResolver());
        // Based on ParameterSetter
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
              getStateManager(),
              ammd.getAbsoluteFieldNumber());
        } else {
          mapping.setObject(
              fieldManager.getObjectManager(), fieldManager.getEntity(), NOT_USED, value);
        }
        // Make sure the field is wrapped where appropriate
        getStateManager().wrapSCOField(ammd.getAbsoluteFieldNumber(), value, false, true, true);
      }
    };

    // If the related object can't exist without the parent it should be part
    // of the parent's entity group.  In order to be part of the parent's
    // entity group we need the parent's key.  In order to get the parent's key
    // we must save the parent before we save the child.  In order to avoid
    // saving the child until after we've saved the parent we register an event
    // that we will apply later.
    if (ammd.isDependent()) {
      storeRelationEvents.add(event);
    } else {
      // not dependent so just apply right away
      event.apply();
    }
  }

  public Object establishEntityGroup() {
    Key parentKey = PARENT_KEY_MAP.get().get(getStateManager().getObject());
    if (parentKey != null && fieldManager.getEntity().getParent() == null) {
      fieldManager.recreateEntityWithAncestor(KeyFactory.encodeKey(parentKey));
      if (getAncestorMemberMetaData() != null) {
        return getAncestorMemberMetaData().getType().equals(Key.class)
            ? parentKey : KeyFactory.encodeKey(parentKey);
      }
    }
    return null;
  }

  Object fetchRelationField(AbstractMemberMetaData ammd) {
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
      // Extract the related key from the entity
      String propName = EntityUtils.getPropertyName(fieldManager.getIdentifierFactory(), ammd);
      Key relatedKey = (Key) fieldManager.getEntity().getProperty(propName);
      value = mapping.getObject(fieldManager.getObjectManager(), relatedKey, NOT_USED);
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
