/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.FieldValues;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.scostore.SetStore;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.EntityUtils;
import com.google.appengine.datanucleus.KeyRegistry;
import com.google.appengine.datanucleus.MetaDataUtils;
import com.google.appengine.datanucleus.Utils;

/**
 * Backing store for sets stored with a "FK" in the element.
 */
public class FKSetStore extends AbstractFKStore implements SetStore {
  public FKSetStore(AbstractMemberMetaData ownerMmd, DatastoreManager storeMgr, ClassLoaderResolver clr) {
    super(ownerMmd, storeMgr, clr);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#hasOrderMapping()
   */
  public boolean hasOrderMapping() {
    return false;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#add(org.datanucleus.store.ObjectProvider, java.lang.Object, int)
   */
  public boolean add(final ObjectProvider op, Object element, int currentSize) {
    if (element == null) {
      // FK sets allow no nulls (since can't have a FK on a null element!)
      throw new NucleusUserException(LOCALISER.msg("056039"));
    }

    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      // Register the parent key for the element when owned
      Key parentKey = EntityUtils.getKeyForObject(op.getObject(), op.getExecutionContext());
      KeyRegistry.getKeyRegistry(op.getExecutionContext()).registerParentKeyForOwnedObject(element, parentKey);
    }

    // Make sure that the element is persisted in the datastore (reachability)
    final Object newOwner = op.getObject();
    ExecutionContext ec = op.getExecutionContext();
    boolean inserted = validateElementForWriting(ec, element, new FieldValues() {
      public void fetchFields(ObjectProvider esm) {
        // Find the (element) table storing the FK back to the owner
        JavaTypeMapping externalFKMapping = elementTable.getExternalMapping(ownerMemberMetaData, 
            MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
        if (externalFKMapping != null) {
          // The element has an external FK mapping so set the value it needs to use in the INSERT
          esm.setAssociatedValue(externalFKMapping, op.getObject());
        }

        if (relationType == RelationType.ONE_TO_MANY_BI) {
          // TODO Move this into RelationshipManager
          // Managed Relations : 1-N bidir, so make sure owner is correct at persist
          Object currentOwner = esm.provideField(getFieldNumberInElementForBidirectional(esm));
          if (currentOwner == null) {
            // No owner, so correct it
            NucleusLogger.PERSISTENCE.info(LOCALISER.msg("056037",
                StringUtils.toJVMIDString(op.getObject()), ownerMemberMetaData.getFullFieldName(), 
                StringUtils.toJVMIDString(esm.getObject())));
            esm.replaceFieldMakeDirty(getFieldNumberInElementForBidirectional(esm), newOwner);
          }
          else if (currentOwner != newOwner && op.getReferencedPC() == null) {
            // Owner of the element is neither this container and not being attached
            // Inconsistent owner, so throw exception
            throw new NucleusUserException(LOCALISER.msg("056038",
                StringUtils.toJVMIDString(op.getObject()), ownerMemberMetaData.getFullFieldName(), 
                StringUtils.toJVMIDString(esm.getObject()),
                StringUtils.toJVMIDString(currentOwner)));
          }
        }
      }

      public void fetchNonLoadedFields(ObjectProvider sm) {}
      public FetchPlan getFetchPlanForLoading() {return null;}
    });

    if (!inserted) {
      // Element was already persistent so make sure the FK is in place
      // TODO This is really "ManagedRelationships" so needs to go in RelationshipManager
      ObjectProvider elementOP = ec.findObjectProvider(element);
      if (relationType == RelationType.ONE_TO_MANY_BI) {
        // Managed Relations : 1-N bidir, so update the owner of the element
        elementOP.isLoaded(getFieldNumberInElementForBidirectional(elementOP)); // Ensure is loaded
        Object oldOwner = elementOP.provideField(getFieldNumberInElementForBidirectional(elementOP));
        if (oldOwner != newOwner) {
          if (NucleusLogger.PERSISTENCE.isDebugEnabled()) {
            NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("055009", StringUtils.toJVMIDString(op.getObject()),
                ownerMemberMetaData.getFullFieldName(), StringUtils.toJVMIDString(element)));
          }

          int relatedFieldNumber = getFieldNumberInElementForBidirectional(elementOP);
          elementOP.replaceFieldMakeDirty(relatedFieldNumber, newOwner);
          if (ec.getManageRelations()) {
            // Managed Relationships - add the change we've made here to be analysed at flush
            ec.getRelationshipManager(elementOP).relationChange(relatedFieldNumber, oldOwner, newOwner);
          }

          if (ec.isFlushing()) {
            elementOP.flush();
          }
        }
        return oldOwner != newOwner;
      }
      else {
        // 1-N unidir so update the FK if not set to be contained in the set
        if (contains(op, element)) {
          return false;
        }
        else {
          if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
            // fk is already set and sets are unindexed so there's nothing else to do
            // Keys (and therefore parents) are immutable so we don't need to ever
            // actually update the parent FK, but we do need to check to make sure
            // someone isn't trying to modify the parent FK
            EntityUtils.checkParentage(element, op);
            return true;
          }
        }
      }
    }
    return true;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#addAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int)
   */
  public boolean addAll(ObjectProvider op, Collection coll, int currentSize) {
    if (coll == null || coll.size() == 0) {
      return false;
    }

    // TODO Investigate if we can do a batch put
    boolean success = false;
    Iterator iter = coll.iterator();
    while (iter.hasNext()) {
      if (add(op, iter.next(), -1)) {
        success = true;
      }
    }

    return success;
  }

  /**
   * Convenience method for whether we should delete elements when clear()/remove() is called.
   * Owned relations will be deleted, whereas unowned will follow the "dependent-element" setting.
   * @return Whether to delete an element on call of clear()/remove()
   */
  protected boolean deleteElementsOnRemoveOrClear() {
    // Removal of child should always delete the child with GAE since cannot null the parent in owned relations
    boolean deleteElements = false;
    boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }

    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      // Field is not dependent, and not nullable so we just delete the elements
      NucleusLogger.DATASTORE.debug(LOCALISER.msg("056035"));
      deleteElements = true;
    } else {
      if (dependent) {
        deleteElements = true;
      }
      // TODO Allow for non-nullable FK
    }
    return deleteElements;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#clear(org.datanucleus.store.ObjectProvider)
   */
  public void clear(ObjectProvider op) {
    // Find elements present in the datastore and process them one-by-one
    boolean deleteElements = deleteElementsOnRemoveOrClear();
    ExecutionContext ec = op.getExecutionContext();
    Iterator elementsIter = iterator(op);
    if (elementsIter != null) {
      while (elementsIter.hasNext()) {
        Object element = elementsIter.next();
        if (ec.getApiAdapter().isPersistable(element) && ec.getApiAdapter().isDeleted(element)) {
          // Element is waiting to be deleted so flush it (it has the FK)
          ObjectProvider elementSM = ec.findObjectProvider(element);
          elementSM.flush();
        } else {
          if (deleteElements) {
            ec.deleteObjectInternal(element);
          } else {
            // TODO Null this out (in parent)
          }
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#iterator(org.datanucleus.store.ObjectProvider)
   */
  public Iterator iterator(ObjectProvider op) {
    ExecutionContext ec = op.getExecutionContext();
    if (MetaDataUtils.readRelatedKeysFromParent(storeMgr, ownerMemberMetaData)) {
      // Get child keys from property in owner Entity if the property exists
      Entity datastoreEntity = getOwnerEntity(op);
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ownerMemberMetaData);
      if (datastoreEntity.hasProperty(propName)) {
        return getChildrenFromParentField(op, ec, -1, -1).listIterator();
      } else {
        if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
          // Not yet got the property in the parent, so this entity has not yet been migrated to latest storage version
          NucleusLogger.PERSISTENCE.info("Collection at field " + ownerMemberMetaData.getFullFieldName() + " of " + op +
              " not yet migrated to latest storage version, so reading elements via the parent key");
        }
      }
    }

    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      // Get child keys by doing a query with the owner as the parent Entity
      ApiAdapter apiAdapter = ec.getApiAdapter();
      Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, op);
      return getChildrenUsingParentQuery(parentKey, Collections.<Query.FilterPredicate>emptyList(),
          Collections.<Query.SortPredicate>emptyList(), ec).iterator();
    } else {
      return Utils.newArrayList().listIterator();
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#remove(org.datanucleus.store.ObjectProvider, java.lang.Object, int, boolean)
   */
  public boolean remove(ObjectProvider op, Object element, int currentSize, boolean allowCascadeDelete) {
    if (element == null) {
        return false;
    }
    if (!validateElementForReading(op.getExecutionContext(), element)) {
        return false;
    }

    // Find the ObjectProvider for the element
    Object elementToRemove = element;
    ExecutionContext ec = op.getExecutionContext();
    if (ec.getApiAdapter().isDetached(element)) {// User passed in detached object to collection.remove()! {
      // Find an attached equivalent of this detached object (DON'T attach the object itself)
      elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
    }
    ObjectProvider elementOP = ec.findObjectProvider(elementToRemove);

    // Check for change of owner of the element (removed from this but added to another one maybe?)
    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      // Check for ownership change when owned relation, and prevent changes
      Object oldOwner = null;
      if (relationType == RelationType.ONE_TO_MANY_BI) {
        if (!ec.getApiAdapter().isDeleted(elementToRemove)) {
          // Find the existing owner if the record hasn't already been deleted
          int elemOwnerFieldNumber = getFieldNumberInElementForBidirectional(elementOP);
          elementOP.isLoaded(elemOwnerFieldNumber);
          oldOwner = elementOP.provideField(elemOwnerFieldNumber);
        }
      }
      if (RelationType.isBidirectional(relationType) && oldOwner != op.getObject() && oldOwner != null) {
        // Owner of the element has been changed, so reject it
        return false;
      }
    }

    boolean deleteElements = deleteElementsOnRemoveOrClear();
    if (ec.getApiAdapter().isPersistable(elementToRemove) && ec.getApiAdapter().isDeleted(elementToRemove)) {
      // Element is waiting to be deleted so flush it (it has the FK)
      elementOP.flush();
    } else {
      if (deleteElements) {
        ec.deleteObjectInternal(elementToRemove);
      } else {
        // TODO Null it out
      }
    }

    return true;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#removeAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int)
   */
  public boolean removeAll(ObjectProvider op, Collection coll, int currentSize) {
    if (coll == null || coll.size() == 0) {
      return false;
    }

    // Check the first element for whether we can null the column or whether we have to delete
    // TODO Investigate if we can do a batch delete
    boolean success = true;
    Iterator iter = coll.iterator();
    while (iter.hasNext()) {
      if (remove(op, iter.next(), -1, true)) {
        success = false;
      }
    }

    return success;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#update(org.datanucleus.store.ObjectProvider, java.util.Collection)
   */
  public void update(ObjectProvider op, Collection coll) {
    if (coll == null || coll.isEmpty()) {
      clear(op);
      return;
    }

    // Find existing elements, and remove any that are no longer present
    Iterator elemIter = iterator(op);
    Collection existing = new HashSet();
    while (elemIter.hasNext()) {
      Object elem = elemIter.next();
      if (!coll.contains(elem)) {
        remove(op, elem, -1, true);
      } else {
        existing.add(elem);
      }
    }

    if (existing.size() != coll.size()) {
      // Add any elements that aren't already present
      Iterator iter = coll.iterator();
      while (iter.hasNext()) {
        Object elem = iter.next();
        if (!existing.contains(elem)) {
          add(op, elem, 0);
        }
      }
    }
  }

  /**
   * This seems to return the field number in the element of the relation when it is a bidirectional relation.
   * @param elementOP ObjectProvider of the element
   * @return The field number in the element for this relation
   */
  protected int getFieldNumberInElementForBidirectional(ObjectProvider elementOP) {
    if (RelationType.isBidirectional(relationType)) {
      AbstractMemberMetaData[] relMmds = ownerMemberMetaData.getRelatedMemberMetaData(clr);
      if (relMmds != null) {
        return relMmds[0].getAbsoluteFieldNumber();
      }
    }
    return -1;
  }
}