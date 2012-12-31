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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.FetchPlan;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.FieldValues;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.mapped.DatastoreClass;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.scostore.ListStore;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.EntityUtils;
import com.google.appengine.datanucleus.KeyRegistry;
import com.google.appengine.datanucleus.MetaDataUtils;
import com.google.appengine.datanucleus.StorageVersion;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.query.LazyResult;

/**
 * Backing store for lists stored with a "FK" in the element.
 */
public class FKListStore extends AbstractFKStore implements ListStore {
  /** Mapping for the ordering column in the element table. */
  protected JavaTypeMapping orderMapping;

  /** Whether the list is indexed (like with JDO). If false then it will have no orderMapping (like with JPA). */
  protected boolean indexedList = true;

  private final ThreadLocal<Boolean> removing = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  public FKListStore(AbstractMemberMetaData ownerMmd, DatastoreManager storeMgr, ClassLoaderResolver clr) {
    super(ownerMmd, storeMgr, clr);

    orderMapping = elementTable.getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX);
    if (ownerMemberMetaData.getOrderMetaData() != null && !ownerMemberMetaData.getOrderMetaData().isIndexedList()) {
        indexedList = false;
    }
    if (!storeMgr.storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS) && 
        orderMapping == null && indexedList) {
      // Early storage version requires that indexedList has an order mapping in the element
      throw new NucleusUserException(LOCALISER.msg("056041", 
          ownerMemberMetaData.getAbstractClassMetaData().getFullClassName(), ownerMemberMetaData.getName(), elementType));
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#hasOrderMapping()
   */
  public boolean hasOrderMapping() {
    return (orderMapping != null);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#add(org.datanucleus.store.ObjectProvider, java.lang.Object, int)
   */
  public boolean add(ObjectProvider ownerOP, Object element, int currentSize) {
    return internalAdd(ownerOP, 0, true, Collections.singleton(element), currentSize);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#addAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int)
   */
  public boolean addAll(ObjectProvider ownerOP, Collection elements, int currentSize) {
    return internalAdd(ownerOP, 0, true, elements, currentSize);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#add(org.datanucleus.store.ObjectProvider, java.lang.Object, int, int)
   */
  public void add(ObjectProvider ownerOP, Object element, int index, int currentSize) {
    internalAdd(ownerOP, index, false, Collections.singleton(element), currentSize);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#addAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int, int)
   */
  public boolean addAll(ObjectProvider ownerOP, Collection elements, int index, int currentSize) {
    return internalAdd(ownerOP, index, false, elements, currentSize);
  }

  /**
   * Internal method for adding an item to the List.
   * @param ownerOP Object Provider of the owner of the list
   * @param startAt The start position
   * @param atEnd Whether to add at the end
   * @param elements The Collection of elements to add.
   * @param currentSize Current size of List (if known). -1 if not known
   * @return Whether it was successful
   */
  protected boolean internalAdd(ObjectProvider ownerOP, int startAt, boolean atEnd, Collection elements, int currentSize) {
    boolean success = false;
    if (elements == null || elements.size() == 0) {
      success = true;
    }
    else {
      if (!storeMgr.storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
        // Check what we have persistent already
        int currentListSize = 0;
        if (currentSize < 0) {
          // Get the current size from the datastore
          currentListSize = size(ownerOP);
        }
        else {
          currentListSize = currentSize;
        }

        boolean shiftingElements = true;
        if (atEnd || startAt == currentListSize) {
          shiftingElements = false;
          startAt = currentListSize; // Not shifting so we insert from the end
        }

        if (shiftingElements)
        {
          // We need to shift existing elements before positioning the new ones
          try {
            // Calculate the amount we need to shift any existing elements by
            // This is used where inserting between existing elements and have to shift down all elements after the start point
            int shift = elements.size();
            // shift up existing elements after start position by "shift"
            for (int i=currentListSize-1; i>=startAt; i--) {
              internalShift(ownerOP, false, i, shift);
            }
          }
          catch (MappedDatastoreException e) {
            // An error was encountered during the shift process so abort here
            throw new NucleusDataStoreException(LOCALISER.msg("056009", e.getMessage()), e.getCause());
          }
        }
      }

      boolean elementsNeedPositioning = false;
      int position = startAt;
      Iterator elementIter = elements.iterator();
      while (elementIter.hasNext()) {
        Object element = elementIter.next();

        if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
          // Register the parent key for the element when owned
          Key parentKey = EntityUtils.getKeyForObject(ownerOP.getObject(), ownerOP.getExecutionContext());
          KeyRegistry.getKeyRegistry(ownerOP.getExecutionContext()).registerParentKeyForOwnedObject(element, parentKey);
        }

        // Persist any non-persistent objects at their final list position (persistence-by-reachability)
        boolean inserted = validateElementForWriting(ownerOP, element, position);
        if (!inserted) {
          if (!storeMgr.storageVersionAtLeast(StorageVersion.WRITE_OWNED_CHILD_KEYS_TO_PARENTS)) {
            // This element wasn't positioned in the validate so we need to set the positions later
            elementsNeedPositioning = true;
          }
        }
        position++;
      }

      if (elementsNeedPositioning) {
        // Some elements have been shifted so the new elements need positioning now, or we already had some
        // of the new elements persistent and so they need their positions setting now
        elementIter = elements.iterator();
        while (elementIter.hasNext()) {
          Object element = elementIter.next();
          updateElementFk(ownerOP, element, ownerOP.getObject(), startAt);
          startAt++;
        }
      }

      success = true;
    }

    return success;
  }

  protected int[] internalShift(ObjectProvider op, boolean batched, int oldIndex, int amount) 
  throws MappedDatastoreException {
    if (orderMapping == null) {
      return null;
    }

    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
    DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
    AbstractClassMetaData acmd = elementCmd;
    String kind =
        storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
    Query q = new Query(kind);
    ExecutionContext ec = op.getExecutionContext();
    Object id = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(op.getInternalObjectId());
    Key key = id instanceof Key ? (Key) id : KeyFactory.stringToKey((String) id);
    q.setAncestor(key);

    // create an entity just to capture the name of the index property
    Entity entity = new Entity(kind);
    orderMapping.setObject(ec, entity, new int[] {1}, oldIndex);
    String indexProp = entity.getProperties().keySet().iterator().next();
    q.addFilter(indexProp, Query.FilterOperator.GREATER_THAN_OR_EQUAL, oldIndex);
    for (Entity shiftMe : service.prepare(service.getCurrentTransaction(null), q).asIterable()) {
      Long pos = (Long) shiftMe.getProperty(indexProp);
      shiftMe.setProperty(indexProp, pos + amount);
      EntityUtils.putEntityIntoDatastore(ec, shiftMe);
    }
    return null;
  }

  /**
   * Utility to update a foreign-key in the element in the case of a unidirectional 1-N relationship.
   * @param op ObjectProvider for the owner
   * @param element The element to update
   * @param owner The owner object to set in the FK
   * @param index The index position (or -1 if not known)
   * @return Whether it was performed successfully
   */
  protected boolean updateElementFk(ObjectProvider op, Object element, Object owner, int index) {
    if (element == null) {
      return false;
    }

    // Keys (and therefore parents) are immutable so we don't need to ever
    // actually update the parent FK, but we do need to check to make sure
    // someone isn't trying to modify the parent FK
    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      EntityUtils.checkParentage(element, op);
    }

    if (orderMapping == null) {
      return false;
    }

    return true;
  }

  /**
   * Convenience method for whether we should delete elements when clear()/remove() is called.
   * @return Whether to delete an element on call of clear()/remove()
   */
  protected boolean deleteElementsOnRemoveOrClear() {
    boolean deleteElements = false;

    boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }

    if (dependent) {
      // Elements are dependent and can't exist on their own, so delete them all
      NucleusLogger.DATASTORE.debug(LOCALISER.msg("056034"));
      deleteElements = true;
    } else {
      if ((ownerMapping.isNullable() && orderMapping == null) ||
          (ownerMapping.isNullable() && orderMapping != null && orderMapping.isNullable())) {
        // Field isn't dependent, and is nullable, so we'll null it
        NucleusLogger.DATASTORE.debug(LOCALISER.msg("056036"));
        deleteElements = false;
      } else {
        if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
          // Field is not dependent, and not nullable so we just delete the elements
          NucleusLogger.DATASTORE.debug(LOCALISER.msg("056035"));
          deleteElements = true;
        } else {
          // Unowned relation doesn't care since FK is not stored
        }
      }
    }

    return deleteElements;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#clear(org.datanucleus.store.ObjectProvider)
   */
  public void clear(ObjectProvider op) {
    boolean deleteElements = deleteElementsOnRemoveOrClear();
    ExecutionContext ec = op.getExecutionContext();
    Iterator elementsIter = iterator(op);
    if (elementsIter != null) {
      while (elementsIter.hasNext()) {
        Object element = elementsIter.next();
        if (ec.getApiAdapter().isPersistable(element) && ec.getApiAdapter().isDeleted(element)) {
          // Element is waiting to be deleted so flush it (it has the FK)
          ObjectProvider objSM = ec.findObjectProvider(element);
          objSM.flush();
        } else {
          if (deleteElements) {
            ec.deleteObjectInternal(element);
          }
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#iterator(org.datanucleus.store.ObjectProvider)
   */
  public Iterator iterator(ObjectProvider op) {
    return listIterator(op);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#listIterator(org.datanucleus.store.ObjectProvider)
   */
  public ListIterator listIterator(ObjectProvider op) {
    return listIterator(op, -1, -1);
  }

  protected ListIterator listIterator(ObjectProvider op, int startIdx, int endIdx) {
    ExecutionContext ec = op.getExecutionContext();
    if (MetaDataUtils.readRelatedKeysFromParent(storeMgr, ownerMemberMetaData)) {
      // Get child keys from property in owner Entity if the property exists
      Entity datastoreEntity = getOwnerEntity(op);
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ownerMemberMetaData);
      if (datastoreEntity.hasProperty(propName)) {
        if (indexedList) {
          return getChildrenFromParentField(op, ec, startIdx, endIdx).listIterator();
        } else if (!MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
          Object value = datastoreEntity.getProperty(propName);
          if (value == null || (value instanceof Collection && ((Collection)value).isEmpty())) {
            // No elements so just return
            return Utils.newArrayList().listIterator();
          }

          return getChildrenByKeys((List<Key>) value, ec); // TODO Use startIdx,endIdx
        } else {
          // TODO Get the objects and then order them in-memory using the order criteria
        }
      } else {
        if (op.getLifecycleState().isDeleted()) {
          // Object has been deleted so just return empty list
          return Utils.newArrayList().listIterator();
        }
      }
    }

    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      // Get child keys by doing a query with the owner as the parent Entity
      Key parentKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), op);
      return getChildrenUsingParentQuery(parentKey, getFilterPredicates(startIdx, endIdx), getSortPredicates(), ec).listIterator();
    } else {
      return Utils.newArrayList().listIterator();
    }
  }

  ListIterator<?> getChildrenByKeys(List<Key> childKeys, final ExecutionContext ec) {
    String kindName = elementTable.getIdentifier().getIdentifierName();
    Query q = new Query(kindName);

    NucleusLogger.PERSISTENCE.debug("Preparing to query for " + childKeys);
    q.addFilter(Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.IN, childKeys);
    for (Query.SortPredicate sp : getSortPredicates()) {
      q.addSort(sp.getPropertyName(), sp.getDirection());
    }

    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);

    Utils.Function<Entity, Object> func = new Utils.Function<Entity, java.lang.Object>() {
      @Override
      public Object apply(Entity from) {
        return EntityUtils.entityToPojo(from, elementCmd, clr, ec, false, ec.getFetchPlan());
      }
    };
    return new LazyResult(ds.prepare(q).asIterable(), func, true).listIterator();
  }

  @Override
  public int size(ObjectProvider op) {
    if (storeMgr.storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS) && !indexedList) {
      if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
        // Ordered list can only be done via parent key currently
        return getSizeUsingParentKeyInChildren(op);
      } else {
        throw new NucleusFatalUserException("Dont currently support ordered lists that are unowned");
      }
    }
    return super.size(op);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#remove(org.datanucleus.store.ObjectProvider, java.lang.Object, int, boolean)
   */
  public boolean remove(ObjectProvider op, Object element, int currentSize, boolean allowCascadeDelete) {
    ExecutionContext ec = op.getExecutionContext();
    if (!validateElementForReading(ec, element)) {
      return false;
    }

    Object elementToRemove = element;
    if (ec.getApiAdapter().isDetached(element)) {
      // Element passed in is detached so find attached version (DON'T attach this object)
      elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false,
          element.getClass().getName());
    }

    return internalRemove(op, elementToRemove, currentSize);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#removeAll(org.datanucleus.store.ObjectProvider, java.util.Collection, int)
   */
  public boolean removeAll(ObjectProvider ownerOP, Collection elements, int currentSize) {
    if (elements == null || elements.size() == 0) {
      return false;
    }

    boolean modified = false;
    if (indexedList) {
      // Get the indices of the elements to remove in reverse order (highest first)
      int[] indices = getIndicesOf(ownerOP, elements);

      // Remove each element in turn, doing the shifting of indexes each time
      // TODO : Change this to remove all in one go and then shift once
      for (int i=0;i<indices.length;i++) {
        removeAt(ownerOP, indices[i], -1);
        modified = true;
      }
    }
    else {
      // Ordered List, so remove the elements
      Iterator iter = elements.iterator();
      while (iter.hasNext()) {
        Object element = iter.next();
        boolean mod = internalRemove(ownerOP, element, -1);
        if (mod) {
          modified = true;
        }
      }
    }

    return modified;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#remove(org.datanucleus.store.ObjectProvider, int, int)
   */
  public Object remove(ObjectProvider ownerOP, int index, int currentSize) {
    Object element = get(ownerOP, index);
    if (indexedList) {
      // Remove the element at this position
      removeAt(ownerOP, index, currentSize);
    }
    else {
      // Ordered list doesn't allow indexed removal so just remove the element
      internalRemove(ownerOP, element, currentSize);
    }

    // TODO This does delete of element, yet internalRemove/removeAt also do
    boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }
    if (dependent && !ownerMemberMetaData.getCollection().isEmbeddedElement()) {
      if (!contains(ownerOP, element)) {
        // Delete the element if it is dependent and doesn't have a duplicate entry in the list
        ownerOP.getExecutionContext().deleteObjectInternal(element);
      }
    }

    return element;
  }

  /**
   * Convenience method to remove the specified element from the List.
   * @param ownerOP ObjectProvider of the owner
   * @param element The element
   * @return Whether the List was modified
   */
  protected boolean internalRemove(ObjectProvider ownerOP, Object element, int size)
  {
    if (indexedList) {
      // Indexed List - The element can be at one position only (no duplicates allowed in FK list)
      int index = indexOf(ownerOP, element);
      if (index == -1) {
        return false;
      }
      removeAt(ownerOP, index, size);
    }
    else {
      // Ordered List - no index so null the FK (if nullable) or delete the element
      ExecutionContext ec = ownerOP.getExecutionContext();
      if (ownerMapping.isNullable()) {
        // Nullify the FK
        ObjectProvider elementSM = ec.findObjectProvider(element);
        if (relationType == RelationType.ONE_TO_MANY_BI) {
          // TODO This is ManagedRelations - move into RelationshipManager
          elementSM.replaceFieldMakeDirty(ownerMemberMetaData.getRelatedMemberMetaData(clr)[0].getAbsoluteFieldNumber(), 
              null);
          if (ec.isFlushing()) {
            elementSM.flush();
          }
        } else {
          updateElementFk(ownerOP, element, null, -1);
          if (deleteElementsOnRemoveOrClear()) {
            // TODO If present elsewhere in List then don't delete the element from persistence
            ec.deleteObjectInternal(element);
          }
        }
      }
      else {
        // Delete the element
        ec.deleteObjectInternal(element);
      }
    }

    return true;
  }

  /**
   * Internal method to remove an object at a location in the List.
   * @param ownerOP ObjectProvider for the owner of the list.
   * @param index The location
   * @param size Current size of list (if known). -1 if not known
   */
  protected void removeAt(ObjectProvider ownerOP, int index, int size)
  {
    if (!indexedList) {
      throw new NucleusUserException("Cannot remove an element from a particular position with an ordered list since no indexes exist");
    }

    // Handle delete/nulling of the element - Use thread-local to prevent recurse
    if (removing.get()) {
      return;
    }
    boolean deleteElement = deleteElementsOnRemoveOrClear();
    ExecutionContext ec = ownerOP.getExecutionContext();
    Object element = get(ownerOP, index);
    try {
      removing.set(true);

      if (!deleteElement) {
        // Nullify the index of the element
        ObjectProvider elementOP = ec.findObjectProvider(element);
        if (elementOP != null && !ec.getApiAdapter().isDeleted(element)) {
          Entity elementEntity = getOwnerEntity(elementOP);
          if (!storeMgr.storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS)) {
            // Remove the external index property from the element
            elementEntity.removeProperty(getIndexPropertyName());
          }
          EntityUtils.putEntityIntoDatastore(ec, elementEntity);
        }
      } else {
        // Delete the element
        ec.deleteObjectInternal(element);
      }
    } finally {
      removing.set(false);
    }

    // TODO Don't bother with this if using latest storage version (but update tests too)
    // Not storing element keys in owner, so need to update the index property of following objects
    if (orderMapping != null) {
      // need to shift indexes of following elements down
      DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
      DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
      AbstractClassMetaData acmd = elementCmd;
      String kind =
        storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
      Query q = new Query(kind);
      Key key = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), ownerOP);
      q.setAncestor(key);

      // create an entity just to capture the name of the index property
      Entity entity = new Entity(kind);
      orderMapping.setObject(ec, entity, new int[] {1}, index);
      String indexProp = entity.getProperties().keySet().iterator().next();
      q.addFilter(indexProp, Query.FilterOperator.GREATER_THAN, index);
      for (Entity shiftMe : service.prepare(service.getCurrentTransaction(null), q).asIterable()) {
        Long pos = (Long) shiftMe.getProperty(indexProp);
        shiftMe.setProperty(indexProp, pos - 1);
        EntityUtils.putEntityIntoDatastore(ec, shiftMe);
      }
    }
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
    Collection existing = new ArrayList();
    Iterator elemIter = iterator(ownerOP);
    while (elemIter.hasNext()) {
      Object elem = elemIter.next();
      if (!coll.contains(elem)) {
        remove(ownerOP, elem, -1, true);
      }
      else {
        existing.add(elem);
      }
    }

    if (existing.equals(coll)) {
      // Existing (after any removals) is same as the specified so job done
      return;
    }

    // TODO Improve this - need to allow for list element position changes etc
    clear(ownerOP);
    addAll(ownerOP, coll, 0);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#get(org.datanucleus.store.ObjectProvider, int)
   */
  public Object get(ObjectProvider op, int index) {
    if (MetaDataUtils.readRelatedKeysFromParent(storeMgr, ownerMemberMetaData)) {
      // Get child keys from field in owner Entity
      ExecutionContext ec = op.getExecutionContext();
      Entity datastoreEntity = getOwnerEntity(op);
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ownerMemberMetaData);
      if (datastoreEntity.hasProperty(propName)) {
        Object value = datastoreEntity.getProperty(propName);
        if (value == null) {
          return null;
        }

        List<Key> keys = (List<Key>)value;
        Key indexKey = keys.get(index);
        DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
        DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);
        try {
          return EntityUtils.entityToPojo(ds.get(indexKey), elementCmd, clr, ec, false, ec.getFetchPlan());
        } catch (EntityNotFoundException enfe) {
          throw new NucleusDataStoreException("Could not determine entity for index=" + index + " with key=" + indexKey, enfe);
        }
      }
    } else {
      // Earlier storage version, for owned relation, so use parentKey for membership of List
      ListIterator iter = listIterator(op, index, index);
      if (iter == null || !iter.hasNext()) {
        return null;
      }

      if (!indexedList) {
        // Restrict to the actual element since can't be done in the query
        Object obj = null;
        int position = 0;
        while (iter.hasNext()) {
          obj = iter.next();
          if (position == index) {
            return obj;
          }
          position++;
        }
      }

      return iter.next();
    }
    return null;
  }

  /**
   * Utility to find the indices of a collection of elements.
   * The returned list are in reverse order (highest index first).
   * @param op ObjectProvider for the owner of the list
   * @param elements The elements
   * @return The indices of the elements in the List.
   */
  protected int[] getIndicesOf(ObjectProvider op, Collection elements)
  {
    if (elements == null || elements.size() == 0) {
      return null;
    }

    ExecutionContext ec = op.getExecutionContext();
    Iterator iter = elements.iterator();
    while (iter.hasNext()) {
      validateElementForReading(ec, iter.next());
    }

    // Since the datastore doesn't support 'or', we're going to sort the keys in memory.
    // issue an ancestor query that fetches all children between the first key and the last, 
    // and then build the array of indices from there. The query may return entities that are 
    // not in the elements so we have to be careful.
    if (elements.isEmpty()) {
      return new int[0];
    }

    if (MetaDataUtils.readRelatedKeysFromParent(storeMgr, ownerMemberMetaData)) {
      // Obtain via field of List<Key> in parent
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ownerMemberMetaData);
      Entity ownerEntity = getOwnerEntity(op);
      if (ownerEntity.hasProperty(propName)) {
        Object value = ownerEntity.getProperty(propName);
        if (value == null) {
          return new int[0];
        }

        // Convert elements into list of keys to search for
        List<Key> keys = (List<Key>) value;
        Set<Key> elementKeys = Utils.newHashSet();
        for (Object element : elements) {
          Key key = EntityUtils.getKeyForObject(element, ec);
          if (key != null) {
            elementKeys.add(key);
          }
        }

        // Generate indices list for these elements
        int i = 0;
        List<Integer> indicesList = new ArrayList<Integer>();
        for (Key key : keys) {
          if (elementKeys.contains(key)) {
            indicesList.add(i);
          }
          i++;
        }
        int[] indices = new int[indicesList.size()];
        i = 0;
        for (Integer index : indicesList) {
          indices[i++] = index;
        }

        return indices;
      }
    } else {
      // Owned relation in earlier storage version so use parentKey to determine membership of list
      List<Key> keys = Utils.newArrayList();
      Set<Key> keySet = Utils.newHashSet();
      for (Object ele : elements) {
        ApiAdapter apiAdapter = ec.getApiAdapter();
        Object keyOrString =
          apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(ele));
        Key key = keyOrString instanceof Key ? (Key) keyOrString : KeyFactory.stringToKey((String) keyOrString);
        if (key == null) {
          throw new NucleusUserException("Collection element does not have a primary key.");
        } else if (key.getParent() == null) {
          throw new NucleusUserException("Collection element primary key does not have a parent.");
        }
        keys.add(key);
        keySet.add(key);
      }
      Collections.sort(keys);
      AbstractClassMetaData emd = elementCmd;
      String kind =
        storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(emd).getIdentifierName();
      Query q = new Query(kind);
      // This is safe because we know we have at least one element and therefore
      // at least one key.
      q.setAncestor(keys.get(0).getParent());
      q.addFilter(
          Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.GREATER_THAN_OR_EQUAL, keys.get(0));
      q.addFilter(
          Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.LESS_THAN_OR_EQUAL, keys.get(keys.size() - 1));
      q.addSort(Entity.KEY_RESERVED_PROPERTY, Query.SortDirection.DESCENDING);
      DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
      DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
      int[] indices = new int[keys.size()];
      int index = 0;
      for (Entity e : service.prepare(service.getCurrentTransaction(null), q).asIterable()) {
        if (keySet.contains(e.getKey())) {
          Long indexVal = (Long) orderMapping.getObject(ec, e, new int[1]);
          if (indexVal == null) {
            throw new NucleusDataStoreException("Null index value");
          }
          indices[index++] = indexVal.intValue();
        }
      }
      if (index != indices.length) {
        // something was missing in the result set
        throw new NucleusDataStoreException("Too few keys returned.");
      }
      return indices;
    }
    return new int[0];
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#indexOf(org.datanucleus.store.ObjectProvider, java.lang.Object)
   */
  public int indexOf(ObjectProvider op, Object element) {
    ExecutionContext ec = op.getExecutionContext();
    validateElementForReading(ec, element);

    ObjectProvider elementOP = ec.findObjectProvider(element);
    Key elementKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), elementOP);
    if (elementKey == null) {
      // Not persistent
      return -1;
    }

    if (MetaDataUtils.readRelatedKeysFromParent(storeMgr, ownerMemberMetaData)) {
      // Return the position using the field of List<Key> in the owner
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ownerMemberMetaData);
      Entity ownerEntity = getOwnerEntity(op);
      if (ownerEntity.hasProperty(propName)) {
        Object value = ownerEntity.getProperty(propName);
        if (value == null) {
          return -1;
        }
        List<Key> keys = (List<Key>) value;
        return keys.indexOf(elementKey);
      }
    } else {
      // Owned relation in earlier storage version so use parentKey to determine membership of list (only present once)
      if (elementKey.getParent() == null) {
        throw new NucleusUserException("Element primary-key does not have a parent.");
      }

      DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
      DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
      try {
        Entity e = service.get(elementKey);
        Long indexVal = (Long) orderMapping.getObject(ec, e, new int[1]);
        if (indexVal == null) {
          throw new NucleusDataStoreException("Null index value");
        }
        return indexVal.intValue();
      } catch (EntityNotFoundException enfe) {
        throw new NucleusDataStoreException("Could not determine index of entity.", enfe);
      }
    }
    return -1;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#lastIndexOf(org.datanucleus.store.ObjectProvider, java.lang.Object)
   */
  public int lastIndexOf(ObjectProvider op, Object element) {
    ExecutionContext ec = op.getExecutionContext();
    validateElementForReading(ec, element);
    ObjectProvider elementOP = ec.findObjectProvider(element);
    Key elementKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), elementOP);
    if (elementKey == null) {
      // Not persistent
      return -1;
    }

    if (MetaDataUtils.readRelatedKeysFromParent(storeMgr, ownerMemberMetaData)) {
      // Return the position using the field of List<Key> in the owner
      String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), ownerMemberMetaData);
      Entity ownerEntity = getOwnerEntity(op);
      if (ownerEntity.hasProperty(propName)) {
        Object value = ownerEntity.getProperty(propName);
        if (value == null) {
          return -1;
        }
        List<Key> keys = (List<Key>) value;
        return keys.lastIndexOf(elementKey);
      }
    }
    // Owned relation in earlier storage version so use parentKey to determine membership of list (only present once)
    return indexOf(op, element);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#set(org.datanucleus.store.ObjectProvider, int, java.lang.Object, boolean)
   */
  public Object set(ObjectProvider op, int index, Object element, boolean allowCascadeDelete) {
    // Get current element at this position
    Object obj = get(op, index);

    if (MetaDataUtils.isOwnedRelation(ownerMemberMetaData, storeMgr)) {
      // Register the parent key for the element when owned
      Key parentKey = EntityUtils.getKeyForObject(op.getObject(), op.getExecutionContext());
      KeyRegistry.getKeyRegistry(op.getExecutionContext()).registerParentKeyForOwnedObject(element, parentKey);
    }

    // Make sure the element going to this position is persisted (and give it its index)
    validateElementForWriting(op, element, index);

    // TODO Allow for a user setting position x as element1 and then setting element2 (that used to be there) to position y
    // At the moment we just delete the previous element
    if (ownerMemberMetaData.getCollection().isDependentElement() && allowCascadeDelete && obj != null) {
      op.getExecutionContext().deleteObjectInternal(obj);
    }

    return obj;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#subList(org.datanucleus.store.ObjectProvider, int, int)
   */
  public List subList(ObjectProvider op, int startIdx, int endIdx) {
    ListIterator iter = listIterator(op, startIdx, endIdx);
    java.util.List list = new ArrayList();
    while (iter.hasNext()) {
      list.add(iter.next());
    }
    if (!indexedList) {
      if (list.size() > (endIdx-startIdx)) {
        // Iterator hasn't restricted what is returned so do the index range restriction here
        return list.subList(startIdx, endIdx);
      }
    }
    return list;
  }

  private List<Query.FilterPredicate> getFilterPredicates(int startIdx, int endIdx) {
    List<Query.FilterPredicate> filterPredicates = Utils.newArrayList();
    if (indexedList) {
      String indexProperty = getIndexPropertyName();
      if (startIdx >= 0 && endIdx == startIdx) {
        // Particular index required so add restriction
        Query.FilterPredicate filterPred =
            new Query.FilterPredicate(indexProperty, Query.FilterOperator.EQUAL, startIdx);
        filterPredicates.add(filterPred);
      } else if (startIdx != -1 || endIdx != -1) {
        // Add restrictions on start/end indices as required
        if (startIdx >= 0) {
          Query.FilterPredicate filterPred =
              new Query.FilterPredicate(indexProperty, Query.FilterOperator.GREATER_THAN_OR_EQUAL, startIdx);
          filterPredicates.add(filterPred);
        }
        if (endIdx >= 0) {
          Query.FilterPredicate filterPred =
              new Query.FilterPredicate(indexProperty, Query.FilterOperator.LESS_THAN, endIdx);
          filterPredicates.add(filterPred);
        }
      }
    }
    return filterPredicates;
  }

  private String getIndexPropertyName() {
    String propertyName;
    if (orderMapping.getMemberMetaData() == null) {
      // I'm not sure what we should do if this mapping doesn't exist so for now we'll just blow up.
      propertyName =
          orderMapping.getDatastoreMappings()[0].getDatastoreField().getIdentifier().getIdentifierName();
    } else {
      propertyName = orderMapping.getMemberMetaData().getName();
      AbstractMemberMetaData ammd = orderMapping.getMemberMetaData();

      if (ammd.getColumn() != null) {
        propertyName = ammd.getColumn();
      } else if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length == 1) {
        propertyName = ammd.getColumnMetaData()[0].getName();
      }
    }
    return propertyName;
  }

  private List<Query.SortPredicate> getSortPredicates() {
    // TODO(maxr) Correctly translate field names to datastore property names
    // (embedded fields, overridden column names, etc.)
    List<Query.SortPredicate> sortPredicates = Utils.newArrayList();
    if (indexedList) {
      // Order by the index column
      String propertyName = getIndexPropertyName();
      Query.SortPredicate sortPredicate =
          new Query.SortPredicate(propertyName, Query.SortDirection.ASCENDING);
      sortPredicates.add(sortPredicate);
    } else {
      for (OrderMetaData.FieldOrder fieldOrder : ownerMemberMetaData.getOrderMetaData().getFieldOrders()) {
        AbstractMemberMetaData orderMmd = elementCmd.getMetaDataForMember(fieldOrder.getFieldName());
        String orderPropName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), orderMmd);
        boolean isPrimaryKey = orderMmd.isPrimaryKey();
        if (isPrimaryKey) {
          if (fieldOrder.isForward() && sortPredicates.isEmpty()) {
            // Don't even bother adding if the first sort is id ASC (this is the
            // default sort so there's no point in making the datastore figure this out).
            break;
          }
          // sorting by id requires us to use a reserved property name
          orderPropName = Entity.KEY_RESERVED_PROPERTY;
        }
        Query.SortPredicate sortPredicate = new Query.SortPredicate(
            orderPropName, fieldOrder.isForward() ? Query.SortDirection.ASCENDING : Query.SortDirection.DESCENDING);
        sortPredicates.add(sortPredicate);
        if (isPrimaryKey) {
          // User wants to sort by pk.  Since pk is guaranteed to be unique, break
          // because we know there's no point in adding any more sort predicates
          break;
        }
      }
    }
    return sortPredicates;
  }

  boolean isPrimaryKey(String propertyName) {
    return elementCmd.getMetaDataForMember(propertyName).isPrimaryKey();
  }

  /**
   * Method to validate that an element is valid for writing to the datastore.
   * TODO Minimise differences to super.validateElementForWriting()
   * @param op ObjectProvider for the owner of the List
   * @param element The element to validate
   * @param index The position that the element is being stored at in the list
   * @return Whether the element was inserted
   */
  protected boolean validateElementForWriting(final ObjectProvider op, Object element, final int index)
  {
    final Object newOwner = op.getObject();

    // Check if element is ok for use in the datastore, specifying any external mappings that may be required
    boolean inserted = super.validateElementForWriting(op.getExecutionContext(), element, new FieldValues()
    {
      public void fetchFields(ObjectProvider elementOP)
      {
        // Find the (element) table storing the FK back to the owner
        boolean isPersistentInterface = storeMgr.getNucleusContext().getMetaDataManager().isPersistentInterface(elementType);
        DatastoreClass elementTable = null;
        if (isPersistentInterface) {
          elementTable = storeMgr.getDatastoreClass(
              storeMgr.getNucleusContext().getMetaDataManager().getImplementationNameForPersistentInterface(elementType), clr);
        }
        else {
          elementTable = storeMgr.getDatastoreClass(elementType, clr);
        }
        if (elementTable == null) {
          // "subclass-table", persisted into table of other class
          AbstractClassMetaData[] managingCmds = storeMgr.getClassesManagingTableForClass(elementCmd, clr);
          if (managingCmds != null && managingCmds.length > 0) {
            // Find which of these subclasses is appropriate for this element
            for (int i=0;i<managingCmds.length;i++) {
              Class tblCls = clr.classForName(managingCmds[i].getFullClassName());
              if (tblCls.isAssignableFrom(elementOP.getObject().getClass())) {
                elementTable = storeMgr.getDatastoreClass(managingCmds[i].getFullClassName(), clr);
                break;
              }
            }
          }
        }

        if (elementTable != null) {
          JavaTypeMapping externalFKMapping = elementTable.getExternalMapping(ownerMemberMetaData, MappingConsumer.MAPPING_TYPE_EXTERNAL_FK);
          if (externalFKMapping != null) {
            // The element has an external FK mapping so set the value it needs to use in the INSERT
            elementOP.setAssociatedValue(externalFKMapping, op.getObject());
          }

          if (orderMapping != null && index >= 0) {
            if (ownerMemberMetaData.getOrderMetaData() != null && ownerMemberMetaData.getOrderMetaData().getMappedBy() != null) {
              // Order is stored in a field in the element so update it
              // We support mapped-by fields of types int/long/Integer/Long currently
              Object indexValue = null;
              if (orderMapping.getMemberMetaData().getTypeName().equals(ClassNameConstants.JAVA_LANG_LONG) ||
                  orderMapping.getMemberMetaData().getTypeName().equals(ClassNameConstants.LONG)) {
                indexValue = Long.valueOf(index);
              } else {
                indexValue = Integer.valueOf(index);
              }
              elementOP.replaceFieldMakeDirty(orderMapping.getMemberMetaData().getAbsoluteFieldNumber(), indexValue);
            } else {
              // Order is stored in a surrogate column so save its vaue for the element to use later
              elementOP.setAssociatedValue(orderMapping, Integer.valueOf(index));
            }
          }
        }
        if (relationType == RelationType.ONE_TO_MANY_BI) {
          // TODO This is ManagedRelations - move into RelationshipManager
          // Managed Relations : 1-N bidir, so make sure owner is correct at persist
          Object currentOwner = elementOP.provideField(elementMemberMetaData.getAbsoluteFieldNumber());
          if (currentOwner == null) {
            // No owner, so correct it
            NucleusLogger.PERSISTENCE.info(LOCALISER.msg("056037",
                StringUtils.toJVMIDString(op.getObject()), ownerMemberMetaData.getFullFieldName(), 
                StringUtils.toJVMIDString(elementOP.getObject())));
            elementOP.replaceFieldMakeDirty(elementMemberMetaData.getAbsoluteFieldNumber(), newOwner);
          }
          else if (currentOwner != newOwner && op.getReferencedPC() == null) {
            // Owner of the element is neither this container nor is it being attached
            // Inconsistent owner, so throw exception
            throw new NucleusUserException(LOCALISER.msg("056038",
                StringUtils.toJVMIDString(op.getObject()), ownerMemberMetaData.getFullFieldName(), 
                StringUtils.toJVMIDString(elementOP.getObject()),
                StringUtils.toJVMIDString(currentOwner)));
          }
        }
      }
      public void fetchNonLoadedFields(ObjectProvider elementOP) {}
      public FetchPlan getFetchPlanForLoading() { return null; }
    });

    return inserted;
  }
}