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
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.scostore.SetStore;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.EntityUtils;

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
  public boolean add(final ObjectProvider ownerOP, Object element, int currentSize) {
    if (element == null) {
      // FK sets allow no nulls (since can't have a FK on a null element!)
      throw new NucleusUserException(LOCALISER.msg("056039"));
    }

    // Make sure that the element is persisted in the datastore (reachability)
    final Object newOwner = ownerOP.getObject();
    ExecutionContext ec = ownerOP.getExecutionContext();
    boolean inserted = validateElementForWriting(ec, element, new FieldValues() {
      public void fetchFields(ObjectProvider esm) {
        // Find the (element) table storing the FK back to the owner
        if (elementTable == null) {
          // "subclass-table", persisted into table of other class
          AbstractClassMetaData[] managingCmds = storeMgr.getClassesManagingTableForClass(elementCmd, clr);
          if (managingCmds != null && managingCmds.length > 0) {
            // Find which of these subclasses is appropriate for this element
            for (int i=0;i<managingCmds.length;i++) {
              Class tblCls = clr.classForName(managingCmds[i].getFullClassName());
              if (tblCls.isAssignableFrom(esm.getObject().getClass())) {
                elementTable = storeMgr.getDatastoreClass(managingCmds[i].getFullClassName(), clr);
                break;
              }
            }
          }
        }

        if (elementTable != null) {
          JavaTypeMapping externalFKMapping = elementTable.getExternalMapping(ownerMemberMetaData, 
              MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
          if (externalFKMapping != null) {
            // The element has an external FK mapping so set the value it needs to use in the INSERT
            esm.setAssociatedValue(externalFKMapping, ownerOP.getObject());
          }
        }

        if (Relation.isBidirectional(relationType)) {
          // TODO Move this into RelationshipManager
          // Managed Relations : 1-N bidir, so make sure owner is correct at persist
          Object currentOwner = esm.provideField(getFieldNumberInElementForBidirectional(esm));
          if (currentOwner == null) {
            // No owner, so correct it
            NucleusLogger.PERSISTENCE.info(LOCALISER.msg("056037",
                ownerOP.toPrintableID(), ownerMemberMetaData.getFullFieldName(), 
                StringUtils.toJVMIDString(esm.getObject())));
            esm.replaceFieldMakeDirty(getFieldNumberInElementForBidirectional(esm), newOwner);
          }
          else if (currentOwner != newOwner && ownerOP.getReferencedPC() == null) {
            // Owner of the element is neither this container and not being attached
            // Inconsistent owner, so throw exception
            throw new NucleusUserException(LOCALISER.msg("056038",
                ownerOP.toPrintableID(), ownerMemberMetaData.getFullFieldName(), 
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
      if (Relation.isBidirectional(relationType)) {
        // Managed Relations : 1-N bidir, so update the owner of the element
        ec.getApiAdapter().isLoaded(elementOP, getFieldNumberInElementForBidirectional(elementOP)); // Ensure is loaded
        Object oldOwner = elementOP.provideField(getFieldNumberInElementForBidirectional(elementOP));
        if (oldOwner != newOwner) {
          if (NucleusLogger.PERSISTENCE.isDebugEnabled()) {
            NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("055009", ownerOP.toPrintableID(),
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
        if (contains(ownerOP, element)) {
          return false;
        }
        else {
          // fk is already set and sets are unindexed so there's nothing else to do
          // Keys (and therefore parents) are immutable so we don't need to ever
          // actually update the parent FK, but we do need to check to make sure
          // someone isn't trying to modify the parent FK
          EntityUtils.checkParentage(element, ownerOP);
          return true;
        }
      }
    }
    return true;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#addAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int)
   */
  public boolean addAll(ObjectProvider ownerOP, Collection coll, int currentSize) {
    if (coll == null || coll.size() == 0) {
      return false;
    }

    // TODO Investigate if we can do a batch put
    boolean success = false;
    Iterator iter = coll.iterator();
    while (iter.hasNext()) {
      if (add(ownerOP, iter.next(), -1)) {
        success = true;
      }
    }

    return success;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#clear(org.datanucleus.store.ObjectProvider)
   */
  public void clear(ObjectProvider ownerOP) {
    // Find elements present in the datastore and delete them one-by-one
    // Removal of child should always delete the child with GAE since cannot null the parent
    ExecutionContext ec = ownerOP.getExecutionContext();
    Iterator elementsIter = iterator(ownerOP);
    if (elementsIter != null) {
      while (elementsIter.hasNext()) {
        Object element = elementsIter.next();
        if (ec.getApiAdapter().isPersistable(element) && ec.getApiAdapter().isDeleted(element)) {
          // Element is waiting to be deleted so flush it (it has the FK)
          ObjectProvider elementSM = ec.findObjectProvider(element);
          elementSM.flush();
        }
        else {
          // Element not yet marked for deletion so go through the normal process
          ec.deleteObjectInternal(element);
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#contains(org.datanucleus.store.ObjectProvider, java.lang.Object)
   */
  public boolean contains(ObjectProvider ownerOP, Object element) {
    ExecutionContext ec = ownerOP.getExecutionContext();
    if (!validateElementForReading(ec, element)) {
      return false;
    }

    // Since we only support owned relationships right now, we can check containment simply 
    // by looking to see if the element's Key contains the parent Key.
    Key childKey = extractElementKey(ec, element);
    // Child key can be null if element has not yet been persisted
    if (childKey == null || childKey.getParent() == null) {
      return false;
    }
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), ownerOP);
    return childKey.getParent().equals(parentKey);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#iterator(org.datanucleus.store.ObjectProvider)
   */
  public Iterator iterator(ObjectProvider op) {
    ExecutionContext ec = op.getExecutionContext();
    ApiAdapter apiAdapter = ec.getApiAdapter();
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, op);
    return getChildren(parentKey, Collections.<Query.FilterPredicate>emptyList(),
        Collections.<Query.SortPredicate>emptyList(), ec).iterator();
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#remove(org.datanucleus.store.ObjectProvider, java.lang.Object, int, boolean)
   */
  public boolean remove(ObjectProvider ownerOP, Object element, int currentSize, boolean allowCascadeDelete) {
    if (element == null) {
        return false;
    }
    if (!validateElementForReading(ownerOP.getExecutionContext(), element)) {
        return false;
    }

    // Find the state manager for the element
    Object elementToRemove = element;
    ExecutionContext ec = ownerOP.getExecutionContext();
    if (ec.getApiAdapter().isDetached(element)) {// User passed in detached object to collection.remove()! {
      // Find an attached equivalent of this detached object (DON'T attach the object itself)
      elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false, element.getClass().getName());
    }

    ObjectProvider elementOP = ec.findObjectProvider(elementToRemove);
    Object oldOwner = null;
    if (Relation.isBidirectional(relationType)) {
      if (!ec.getApiAdapter().isDeleted(elementToRemove)) {
        // Find the existing owner if the record hasn't already been deleted
        int elemOwnerFieldNumber = getFieldNumberInElementForBidirectional(elementOP);
        ec.getApiAdapter().isLoaded(elementOP, elemOwnerFieldNumber);
        oldOwner = elementOP.provideField(elemOwnerFieldNumber);
      }
    }
    else {
      // TODO Check if the element is managed by a different owner now
    }

    // Owner of the element has been changed
    if (Relation.isBidirectional(relationType) && oldOwner != ownerOP.getObject() && oldOwner != null) {
      // TODO Handle this. Can't swap owner of elements in GAE
      return false;
    }

    // Delete the element since cannot remove it and null the parent
    if (ec.getApiAdapter().isPersistable(elementToRemove) && ec.getApiAdapter().isDeleted(elementToRemove)) {
      // Element is waiting to be deleted so flush it (it has the FK)
      elementOP.flush();
    }
    else {
      // Element not yet marked for deletion so go through the normal process
      ec.deleteObjectInternal(elementToRemove);
    }

    return true;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#removeAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int)
   */
  public boolean removeAll(ObjectProvider ownerOP, Collection coll, int currentSize) {
    if (coll == null || coll.size() == 0) {
      return false;
    }

    // Check the first element for whether we can null the column or whether we have to delete
    // TODO Investigate if we can do a batch delete
    boolean success = true;
    Iterator iter = coll.iterator();
    while (iter.hasNext()) {
      if (remove(ownerOP, iter.next(), -1, true)) {
        success = false;
      }
    }

    return success;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#update(org.datanucleus.store.ObjectProvider, java.util.Collection)
   */
  public void update(ObjectProvider ownerOP, Collection coll) {
    if (coll == null || coll.isEmpty()) {
      clear(ownerOP);
      return;
    }

    // Find existing elements, and remove any that are no longer present
    Iterator elemIter = iterator(ownerOP);
    Collection existing = new HashSet();
    while (elemIter.hasNext()) {
      Object elem = elemIter.next();
      if (!coll.contains(elem)) {
        remove(ownerOP, elem, -1, true);
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
          add(ownerOP, elem, 0);
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
    return (relationType == Relation.ONE_TO_MANY_BI ? 
        elementOP.getClassMetaData().getAbsolutePositionOfMember(ownerMemberMetaData.getMappedBy()) : -1);
  }

  protected Key extractElementKey(ExecutionContext ec, Object element) {
    ApiAdapter apiAdapter = ec.getApiAdapter();
    Object id = apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(element));
    if (id == null) {
      return null;
    }
    // This is a child object so we know the pk is Key or encoded String
    return id instanceof Key ? (Key) id : KeyFactory.stringToKey((String) id);
  }
}