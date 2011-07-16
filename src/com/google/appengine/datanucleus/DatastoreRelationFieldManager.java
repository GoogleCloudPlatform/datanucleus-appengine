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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaData;
import org.datanucleus.metadata.NullValue;
import org.datanucleus.metadata.Relation;

import com.google.appengine.datanucleus.mapping.DatastoreTable;
import com.google.appengine.datanucleus.mapping.InsertMappingConsumer;
import com.google.appengine.datanucleus.query.DatastoreQuery;

import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.exceptions.NotYetFlushedException;
import org.datanucleus.store.mapped.mapping.EmbeddedPCMapping;
import org.datanucleus.store.mapped.mapping.InterfaceMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.mapped.mapping.SerialisedPCMapping;
import org.datanucleus.store.mapped.mapping.SerialisedReferenceMapping;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreRelationFieldManager {

  // Needed for relation management in datanucleus.
  private static final int[] NOT_USED = {0};
  public static final int IS_PARENT_VALUE = -1;
  private static final int[] IS_PARENT_VALUE_ARR = {IS_PARENT_VALUE};
  public static final String PARENT_KEY_PROPERTY = "____PARENT_KEY____";

  public static final int IS_FK_VALUE = -2;
  private static final int[] IS_FK_VALUE_ARR = {IS_FK_VALUE};

  private final DatastoreFieldManager fieldManager;

  // Events that know how to store relations.
  private final List<StoreRelationEvent> storeRelationEvents = Utils.newArrayList();

  DatastoreRelationFieldManager(DatastoreFieldManager fieldManager) {
    this.fieldManager = fieldManager;
  }

  /**
   * Applies all the relation events that have been built up.
   * @return {@code true} if the relations changed in a way that
   * requires an update to the relation owner, {@code false} otherwise.
   */
  boolean storeRelations(KeyRegistry keyRegistry) {
      NucleusLogger.GENERAL.info(">> RelationFM.storeRelations ");
    if (storeRelationEvents.isEmpty()) {
      return false;
    }
    if (fieldManager.getEntity().getKey() != null) {
      keyRegistry.registerKey(getObjectProvider(), fieldManager);
    }
    for (StoreRelationEvent event : storeRelationEvents) {
      event.apply();
    }
    storeRelationEvents.clear();
    try {
      return keyRegistry.parentNeedsUpdate(fieldManager.getEntity().getKey());
    } finally {
      keyRegistry.clearModifiedParent(fieldManager.getEntity().getKey());
    }
  }

  private DatastoreManager getStoreManager() {
    return fieldManager.getStoreManager();
  }

  void storeRelationField(final AbstractClassMetaData acmd,
                          final AbstractMemberMetaData ammd, final Object value,
                          final boolean isInsert, final InsertMappingConsumer consumer) {
    NucleusLogger.GENERAL.info(">> RelationFM.storeRelationField " + ammd.getFullFieldName() + " value=" + StringUtils.toJVMIDString(value));
    StoreRelationEvent event = new StoreRelationEvent() {
      public void apply() {
        DatastoreTable table = fieldManager.getDatastoreTable();

        ObjectProvider op = getObjectProvider();
        int fieldNumber = ammd.getAbsoluteFieldNumber();
        // Based on ParameterSetter
        try {
          JavaTypeMapping mapping = table.getMemberMappingInDatastoreClass(ammd);
          if (mapping instanceof EmbeddedPCMapping ||
              mapping instanceof SerialisedPCMapping ||
              mapping instanceof SerialisedReferenceMapping ||
              mapping instanceof PersistableMapping ||
              mapping instanceof InterfaceMapping) {
            setObjectViaMapping(mapping, table, value, op, fieldNumber);
            op.wrapSCOField(fieldNumber, value, false, true, true);
          } else {
            if (isInsert) {
              runPostInsertMappingCallbacks(consumer, value);
            } else {
              runPostUpdateMappingCallbacks(consumer);
            }
          }
        } catch (NotYetFlushedException e) {
          if (acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getNullValue() == NullValue.EXCEPTION) {
            throw e;
          }
          op.updateFieldAfterInsert(e.getPersistable(), fieldNumber);
        }
      }

      private void setObjectViaMapping(JavaTypeMapping mapping, DatastoreTable table, Object value,
          ObjectProvider op, int fieldNumber) {
        boolean fieldIsParentKeyProvider = table.isParentKeyProvider(ammd);
        if (!fieldIsParentKeyProvider) {
          checkForParentSwitch(value, op);
        }
        Entity entity = fieldManager.getEntity();
        mapping.setObject(
            fieldManager.getExecutionContext(),
            entity,
            fieldIsParentKeyProvider ? IS_PARENT_VALUE_ARR : IS_FK_VALUE_ARR,
            value,
            op,
            fieldNumber);

        // If the field we're setting is the one side of an owned many-to-one,
        // its pk needs to be the parent of the key of the entity we're
        // currently populating.  We look for a magic property that tells
        // us if this change needs to be made.  See
        // DatastoreFKMapping.setObject for all the gory details.
        Object parentKeyObj = entity.getProperty(PARENT_KEY_PROPERTY);
        if (parentKeyObj != null) {
          AbstractClassMetaData parentCmd = op.getExecutionContext().getMetaDataManager().getMetaDataForClass(
              ammd.getType(), fieldManager.getClassLoaderResolver());
          Key parentKey = EntityUtils.getPkAsKey(parentKeyObj, parentCmd, fieldManager.getExecutionContext());
          entity.removeProperty(PARENT_KEY_PROPERTY);
          fieldManager.recreateEntityWithParent(parentKey);
        }
      }
    };

    // If the related object can't exist without the parent it should be part
    // of the parent's entity group.  In order to be part of the parent's
    // entity group we need the parent's key.  In order to get the parent's key
    // we must save the parent before we save the child.  In order to avoid
    // saving the child until after we've saved the parent we register an event
    // that we will apply later.
    storeRelationEvents.add(event);
  }

  public static void checkForParentSwitch(Object child, ObjectProvider parentOP) {
    if (child == null) {
      return;
    }
    ExecutionContext ec = parentOP.getExecutionContext();
    ApiAdapter apiAdapter = ec.getApiAdapter();

    ObjectProvider childOP = ec.findObjectProvider(child);
    if (apiAdapter.isNew(child) &&
        (childOP == null || childOP.getAssociatedValue(EntityUtils.getCurrentTransaction(ec)) == null)) {
      // This condition is difficult to get right.  An object that has been persisted
      // (and therefore had its primary key already established) may still be considered
      // NEW by the apiAdapter if there is a txn and the txn has not yet committed.
      // In order to determine if an object has been persisted we see if there is
      // a state manager for it.  If there isn't, there's no way it was persisted.
      // If there is, it's still possible that it hasn't been persisted so we check
      // to see if there is an associated Entity. 
      // TODO Just call sm.isFlushedNew(). It's not that hard
      return;
    }
    // Since we only support owned relationships right now, we can assume
    // that this is parent/child and verify that the parent of the childSM
    // is the parent object in this cascade.
    // We know that the child primary key is a Key or an encoded String
    // because we don't support child objects with primary keys of type
    // Long or unencoded String and our metadata validation would have
    // caught it.
    Object childKeyOrString =
        apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(child));
    if (childKeyOrString == null) {
      // must be a new object or transient
      return;
    }
    Key childKey = childKeyOrString instanceof Key
                   ? (Key) childKeyOrString : KeyFactory.stringToKey((String) childKeyOrString);

    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, parentOP);

    if (childKey.getParent() == null) {
      throw new ChildWithoutParentException(parentKey, childKey);
    } else if (!parentKey.equals(childKey.getParent())) {
      throw new ChildWithWrongParentException(parentKey, childKey);
    }
  }

  private void runPostInsertMappingCallbacks(InsertMappingConsumer consumer, Object value) {
    ObjectProvider op = getObjectProvider();
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      callback.postInsert(op);
    }
  }

  private void runPostUpdateMappingCallbacks(InsertMappingConsumer consumer) {
    ObjectProvider op = getObjectProvider();
    for (MappingCallbacks callback : consumer.getMappingCallbacks()) {
      callback.postUpdate(op);
    }
  }

  Object fetchRelationField(ClassLoaderResolver clr, AbstractMemberMetaData ammd) {
    DatastoreTable dt = fieldManager.getDatastoreTable();
    JavaTypeMapping mapping = dt.getMemberMappingInDatastoreClass(ammd);
    // Based on ResultSetGetter
    Object value;
    if (mapping instanceof EmbeddedPCMapping ||
        mapping instanceof SerialisedPCMapping ||
        mapping instanceof SerialisedReferenceMapping) {
      value = mapping.getObject(
          fieldManager.getExecutionContext(),
          fieldManager.getEntity(),
          NOT_USED,
          getObjectProvider(),
          ammd.getAbsoluteFieldNumber());
    } else {
      int relationType = ammd.getRelationType(clr);
      if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) {
        // Even though the mapping is 1 to 1, we model it as a 1 to many and then
        // just throw a runtime exception if we get multiple children.  We would
        // prefer to store the child id on the parent, but we can't because creating
        // a parent and child at the same time involves 3 distinct writes:
        // 1) We put the parent object in order to get a Key.
        // 2) We put the child object, which needs the Key of the parent as
        // the parent of its own Key so that parent and child reside in the
        // same entity group.
        // 3) We re-put the parent object, adding the Key of the child object
        // as a property on the parent.
        // The problem is that the datastore does not support multiple writes
        // to the same entity within a single transaction, so there's no way
        // to perform this sequence of events atomically, and that's a problem.

        // We have 2 scenarios here.  The first is that we're loading the parent
        // side of a 1 to 1 and we want the child.  In that scenario we're going
        // to issue a parent query against the child table with the expectation
        // that there is either 1 result or 0.

        // The second scearnio is that we're loading the child side of a
        // bidirectional 1 to 1 and we want the parent.  In that scenario
        // the key of the parent is part of the child's key so we can just
        // issue a fetch using the parent's key.
        DatastoreTable table = fieldManager.getDatastoreTable();
        if (table.isParentKeyProvider(ammd)) {
          // bidir 1 to 1 and we are the child
          value = lookupParent(ammd, mapping, false);
        } else {
          // bidir 1 to 1 and we are the parent
          value = lookupOneToOneChild(ammd, clr);
        }
      } else if (relationType == Relation.MANY_TO_ONE_BI) {
        // Do not complain about a non existing parent
        // if we have a self referencing relation
        // and are on the top of the hierarchy.
        MetaData other = ammd.getRelatedMemberMetaData(clr)[0].getParent();
        MetaData parent = ammd.getParent();
        boolean allowNullParent =
            other == parent && fieldManager.getEntity().getKey().getParent() == null;
        value = lookupParent(ammd, mapping, allowNullParent);
      } else {
        value = null;
      }
    }
    // Return the field value (as a wrapper if wrappable)
    return getObjectProvider().wrapSCOField(ammd.getAbsoluteFieldNumber(), value, false, false, false);
  }

  private Object lookupParent(AbstractMemberMetaData ammd, JavaTypeMapping mapping, boolean allowNullParent) {
    Key parentKey = fieldManager.getEntity().getParent();
    if (parentKey == null) {
      if (!allowNullParent) {
        String childClass = fieldManager.getObjectProvider().getClassMetaData().getFullClassName();
        throw new NucleusFatalUserException("Field " + ammd.getFullFieldName() + " should be able to "
            + "provide a reference to its parent but the entity does not have a parent.  "
            + "Did you perhaps try to establish an instance of " + childClass  +  " as "
            + "the child of an instance of " + ammd.getTypeName() + " after the child had already been "
            + "persisted?");
      } else {
        return null;
      }
    }

    return mapping.getObject(getObjectProvider().getExecutionContext(), parentKey, NOT_USED);
  }

  private Object lookupOneToOneChild(AbstractMemberMetaData ammd, ClassLoaderResolver clr) {
    ExecutionContext ec = getObjectProvider().getExecutionContext();
    AbstractClassMetaData childClassMetaData =
        ec.getMetaDataManager().getMetaDataForClass(ammd.getType(), clr);
    String kind = getStoreManager().getIdentifierFactory().newDatastoreContainerIdentifier(
        childClassMetaData).getIdentifierName();
    Entity parentEntity = fieldManager.getEntity();
    // We're going to issue a query for all entities of the given kind with
    // the parent entity's key as their parent.  There should be only 1.
    Query q = new Query(kind, parentEntity.getKey());
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    DatastoreService datastoreService = DatastoreServiceFactoryInternal.getDatastoreService(config);
    // We have to pull back all children because the datastore does not let us
    // filter ancestors by depth and an indirect child could come back before a
    // direct child.  eg:
    // a/b/c
    // a/c
    for (Entity e : datastoreService.prepare(q).asIterable()) {
      if (parentEntity.getKey().equals(e.getKey().getParent())) {
        // TODO(maxr) Figure out how to hook this up to a StateManager!
        return DatastoreQuery.entityToPojo(e, childClassMetaData, clr, ec, false, ec.getFetchPlan());
        // We are potentially ignoring data errors where there is more than one
        // direct child for the one to one.  Unfortunately, in order to detect
        // this we need to read all the way to the end of the Iterable and that
        // might pull back a lot more data than is really necessary.
      }
    }
    return null;
  }

  private ObjectProvider getObjectProvider() {
    return fieldManager.getObjectProvider();
  }
  /**
   * Supports a mechanism for delaying the storage of a relation.
   */
  private interface StoreRelationEvent {
    void apply();
  }

  static class ChildWithoutParentException extends NucleusFatalUserException {
    public ChildWithoutParentException(Key parentKey, Key childKey) {
      super("Detected attempt to establish " + parentKey + " as the "
             + "parent of " + childKey + " but the entity identified by "
             + childKey + " has already been persisted without a parent.  A parent cannot "
             + "be established or changed once an object has been persisted.");
    }
  }

  static class ChildWithWrongParentException extends NucleusFatalUserException {
    public ChildWithWrongParentException(Key parentKey, Key childKey) {
      super("Detected attempt to establish " + parentKey + " as the "
         + "parent of " + childKey + " but the entity identified by "
         + childKey + " is already a child of " + childKey.getParent() + ".  A parent cannot "
         + "be established or changed once an object has been persisted.");
    }
  }
}
