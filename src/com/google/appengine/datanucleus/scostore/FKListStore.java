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
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.ObjectProvider;
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
import com.google.appengine.datanucleus.DatastorePersistenceHandler;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.EntityUtils;
import com.google.appengine.datanucleus.ForceFlushPreCommitTransactionEventListener;
import com.google.appengine.datanucleus.KeyRegistry;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

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
    if (orderMapping == null && indexedList) {
        // "Indexed List" but no order mapping present!
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
   * @param sm The state manager
   * @param startAt The start position
   * @param atEnd Whether to add at the end
   * @param elements The Collection of elements to add.
   * @param currentSize Current size of List (if known). -1 if not known
   * @return Whether it was successful
   */
  protected boolean internalAdd(ObjectProvider ownerOP, int startAt, boolean atEnd, Collection elements, int currentSize) {
    ExecutionContext ec = ownerOP.getExecutionContext();
    boolean success = false;
    if (elements == null || elements.size() == 0) {
      success = true;
    }
    else {
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

      boolean elementsNeedPositioning = false;
      int position = startAt;
      Iterator elementIter = elements.iterator();
      while (elementIter.hasNext()) {
        // Persist any non-persistent objects optionally at their final list position (persistence-by-reachability)
        if (shiftingElements) {
          // We have to shift things so dont bother with positioning
          position = -1;
        }

        boolean inserted = validateElementForWriting(ownerOP, elementIter.next(), position);
        if (!inserted || shiftingElements) {
          // This element wasnt positioned in the validate so we need to set the positions later
          elementsNeedPositioning = true;
        }
        if (!shiftingElements) {
          position++;
        }
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

      if (shiftingElements || elementsNeedPositioning) {
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

    if (success && !ec.getTransaction().isActive()) {
      // TODO Remove this nonsense
      ec.getTransaction().addTransactionEventListener(new ForceFlushPreCommitTransactionEventListener(ownerOP));
      return true;
    }
    return false;
  }

  protected int[] internalShift(ObjectProvider ownerOP, boolean batched, int oldIndex, int amount) 
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
    ExecutionContext ec = ownerOP.getExecutionContext();
    Object id = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(ownerOP.getInternalObjectId());
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
   * @param sm StateManager for the owner
   * @param element The element to update
   * @param owner The owner object to set in the FK
   * @param index The index position (or -1 if not known)
   * @return Whether it was performed successfully
   */
  protected boolean updateElementFk(ObjectProvider sm, Object element, Object owner, int index) {
    if (element == null) {
      return false;
    }

    // Keys (and therefore parents) are immutable so we don't need to ever
    // actually update the parent FK, but we do need to check to make sure
    // someone isn't trying to modify the parent FK
    EntityUtils.checkParentage(element, sm);

    if (orderMapping == null) {
      return false;
    }

    ExecutionContext ec = sm.getExecutionContext();
    ObjectProvider elementOP = ec.findObjectProvider(element);
    // The fk is already set but we still need to set the index
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    // See DatastoreFieldManager.handleIndexFields for info on why this absurdity is necessary.
    Entity entity =
      (Entity) elementOP.getAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED);
    if (entity != null) {
      elementOP.setAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED, null);
      elementOP.setAssociatedValue(orderMapping, index);
      if (entity.getParent() == null) {
        ObjectProvider parentOP = ec.findObjectProvider(owner);
        // need to register the proper parent for this entity
        Key parentKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), parentOP);
        KeyRegistry.getKeyRegistry(ec).registerKey(element, parentKey, elementOP, 
            ownerMemberMetaData.getCollection().getElementType());
      }
      handler.insertObject(elementOP);
    }
    return true;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#clear(org.datanucleus.store.ObjectProvider)
   */
  public void clear(ObjectProvider ownerOP) {
    boolean deleteElements = false;
    ExecutionContext ec = ownerOP.getExecutionContext();

    boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }
    if (dependent) {
      // Elements are dependent and can't exist on their own, so delete them all
      NucleusLogger.DATASTORE.debug(LOCALISER.msg("056034"));
      deleteElements = true;
    }
    else {
      if (ownerMapping.isNullable() && orderMapping == null) {
        // Field is not dependent, and nullable so we null the FK
        NucleusLogger.DATASTORE.debug(LOCALISER.msg("056036"));
        deleteElements = false;
      }
      else if (ownerMapping.isNullable() && orderMapping != null && orderMapping.isNullable()) {
        // Field is not dependent, and nullable so we null the FK
        NucleusLogger.DATASTORE.debug(LOCALISER.msg("056036"));
        deleteElements = false;
      }
      else {
        // Field is not dependent, and not nullable so we just delete the elements
        NucleusLogger.DATASTORE.debug(LOCALISER.msg("056035"));
        deleteElements = true;
      }
    }

    if (deleteElements) {
      // Find elements present in the datastore and delete them one-by-one
      Iterator elementsIter = iterator(ownerOP);
      if (elementsIter != null) {
        while (elementsIter.hasNext()) {
          Object element = elementsIter.next();
          if (ec.getApiAdapter().isPersistable(element) && ec.getApiAdapter().isDeleted(element)) {
            // Element is waiting to be deleted so flush it (it has the FK)
            ObjectProvider objSM = ec.findObjectProvider(element);
            objSM.flush();
          }
          else {
            // Element not yet marked for deletion so go through the normal process
            ec.deleteObjectInternal(element);
          }
        }
      }
    }
    else {
      throw new UnsupportedOperationException("Non-owned relationships are not currently supported");
    }
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#contains(org.datanucleus.store.ObjectProvider, java.lang.Object)
   */
  public boolean contains(ObjectProvider ownerOP, Object element) {
    if (!validateElementForReading(ownerOP.getExecutionContext(), element)) {
      return false;
    }

    // Since we only support owned relationships right now, we can check containment simply by looking 
    // to see if the element's Key contains the parent Key.
    ExecutionContext ec = ownerOP.getExecutionContext();
    Key childKey = extractElementKey(ec, element);
    // Child key can be null if element has not yet been persisted
    if (childKey == null || childKey.getParent() == null) {
      return false;
    }

    return childKey.getParent().equals(EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), ownerOP));
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#iterator(org.datanucleus.store.ObjectProvider)
   */
  public Iterator iterator(ObjectProvider ownerOP) {
    return listIterator(ownerOP);
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#listIterator(org.datanucleus.store.ObjectProvider)
   */
  public ListIterator listIterator(ObjectProvider ownerOP) {
    return listIterator(ownerOP, -1, -1);
  }

  protected ListIterator listIterator(ObjectProvider ownerOP, int startIdx, int endIdx) {
    ExecutionContext ec = ownerOP.getExecutionContext();
    ApiAdapter apiAdapter = ec.getApiAdapter();
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, ownerOP);
    return getChildren(parentKey, getFilterPredicates(startIdx, endIdx), getSortPredicates(), ec).listIterator();
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.CollectionStore#remove(org.datanucleus.store.ObjectProvider, java.lang.Object, int, boolean)
   */
  public boolean remove(ObjectProvider ownerOP, Object element, int currentSize, boolean allowCascadeDelete) {
    if (!validateElementForReading(ownerOP.getExecutionContext(), element)) {
      return false;
    }

    Object elementToRemove = element;
    ExecutionContext ec = ownerOP.getExecutionContext();
    if (ec.getApiAdapter().isDetached(element)) {
      // Element passed in is detached so find attached version (DON'T attach this object)
      elementToRemove = ec.findObject(ec.getApiAdapter().getIdForObject(element), true, false,
          element.getClass().getName());
    }

    boolean modified = internalRemove(ownerOP, elementToRemove, currentSize);

    CollectionMetaData collmd = ownerMemberMetaData.getCollection();
    boolean dependent = collmd.isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }
    if (allowCascadeDelete && dependent && !collmd.isEmbeddedElement()) {
      // Delete the element if it is dependent
      ec.deleteObjectInternal(elementToRemove);
    }

    return modified;
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
      // Ordered List
      // TODO Remove the list item
      throw new NucleusException("Not yet implemented FKListStore.remove for ordered lists");
    }

    boolean dependent = ownerMemberMetaData.getCollection().isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }
    if (dependent) {
      // "delete-dependent" : delete elements if the collection is marked as dependent
      // TODO What if the collection contains elements that are not in the List ? should not delete them
      ownerOP.getExecutionContext().deleteObjects(elements.toArray());
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

    CollectionMetaData collmd = ownerMemberMetaData.getCollection();
    boolean dependent = collmd.isDependentElement();
    if (ownerMemberMetaData.isCascadeRemoveOrphans()) {
      dependent = true;
    }
    if (dependent && !collmd.isEmbeddedElement()) {
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
      // Indexed List
      // The element can be at one position only (no duplicates allowed in FK list)
      int index = indexOf(ownerOP, element);
      if (index == -1) {
        return false;
      }
      removeAt(ownerOP, index, size);
    }
    else {
      // Ordered List - no index so null the FK (if nullable) or delete the element
      if (ownerMapping.isNullable()) {
        // Nullify the FK
        ExecutionContext ec = ownerOP.getExecutionContext();
        ObjectProvider elementSM = ec.findObjectProvider(element);
        if (relationType == Relation.ONE_TO_MANY_BI) {
          // TODO This is ManagedRelations - move into RelationshipManager
          elementSM.replaceFieldMakeDirty(ownerMemberMetaData.getRelatedMemberMetaData(clr)[0].getAbsoluteFieldNumber(), 
              null);
          if (ec.isFlushing()) {
            elementSM.flush();
          }
        }
        // TODO Shouldn't we always null the FK in the datastore, not just when unidirectional?
        else {
          updateElementFk(ownerOP, element, null, -1);
        }
      }
      else {
        // Delete the element
        // TODO Log this
        ownerOP.getExecutionContext().deleteObjectInternal(element);
      }
    }

    return true;
  }

  /**
   * Internal method to remove an object at a location in the List.
   * @param sm The state manager.
   * @param index The location
   * @param size Current size of list (if known). -1 if not known
   */
  protected void removeAt(ObjectProvider ownerOP, int index, int size)
  {
    if (!indexedList) {
      throw new NucleusUserException("Cannot remove an element from a particular position with an ordered list since no indexes exist");
    }

    boolean nullify = false;
    if (ownerMapping.isNullable() && orderMapping != null && orderMapping.isNullable()) {
      NucleusLogger.DATASTORE.debug(LOCALISER.msg("056043"));
      nullify = true;
    }
    else {
      NucleusLogger.DATASTORE.debug(LOCALISER.msg("056042"));
    }

    if (removing.get()) {
      return;
    }

    if (nullify) {
      // we don't support unowned relationships yet
      throw new UnsupportedOperationException("Non-owned relationships are not currently supported.");
    } else {
      // first we need to delete the element
      ExecutionContext ec = ownerOP.getExecutionContext();
      Object element = get(ownerOP, index);
      ObjectProvider elementOP = ec.findObjectProvider(element);
      DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
      // the delete call can end up cascading back here, so set a thread-local
      // to make sure we don't do it more than once
      removing.set(true);
      try {
        handler.deleteObject(elementOP);
      } finally {
        removing.set(false);
      }

      if (orderMapping != null) {
        // need to shift everyone down
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
  public Object get(ObjectProvider ownerOP, int index) {
    ListIterator iter = listIterator(ownerOP, index, index);
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

  /**
   * Utility to find the indices of a collection of elements.
   * The returned list are in reverse order (highest index first).
   * @param ownerOP ObjectProvider for the owner of the list
   * @param elements The elements
   * @return The indices of the elements in the List.
   */
  protected int[] getIndicesOf(ObjectProvider ownerOP, Collection elements)
  {
    if (elements == null || elements.size() == 0) {
      return null;
    }

    ExecutionContext ec = ownerOP.getExecutionContext();
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
        indices[index++] = extractIndexProperty(e, ec);
      }
    }
    if (index != indices.length) {
      // something was missing in the result set
      throw new NucleusDataStoreException("Too few keys returned.");
    }
    return indices;
  }

  private int extractIndexProperty(Entity e, ExecutionContext ec) {
    Long indexVal = (Long) orderMapping.getObject(ec, e, new int[1]);
    if (indexVal == null) {
      throw new NucleusDataStoreException("Null index value");
    }
    return indexVal.intValue();
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#indexOf(org.datanucleus.store.ObjectProvider, java.lang.Object)
   */
  public int indexOf(ObjectProvider ownerOP, Object element) {
    ExecutionContext ec = ownerOP.getExecutionContext();
    validateElementForReading(ec, element);

    ObjectProvider elementOP = ec.findObjectProvider(element);
    Key elementKey = EntityUtils.getPrimaryKeyAsKey(ec.getApiAdapter(), elementOP);
    if (elementKey == null) {
      throw new NucleusUserException("Collection element does not have a primary key.");
    } else if (elementKey.getParent() == null) {
      throw new NucleusUserException("Collection element primary key does not have a parent.");
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

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#lastIndexOf(org.datanucleus.store.ObjectProvider, java.lang.Object)
   */
  public int lastIndexOf(ObjectProvider ownerOP, Object element) {
    validateElementForReading(ownerOP.getExecutionContext(), element);
    // TODO(maxr) Only seems to be called when useCache on the List
    // is false, but it's true in all my tests and it looks like you
    // need to set datanucleus-specific properties to get it to be false.
    // See SCOUtils#useContainerCache.  We'll take care of this later.
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#set(org.datanucleus.store.ObjectProvider, int, java.lang.Object, boolean)
   */
  public Object set(ObjectProvider ownerOP, int index, Object element, boolean allowCascadeDelete) {
    validateElementForWriting(ownerOP, element, -1); // Last argument means dont set the position on any INSERT
    Object obj = get(ownerOP, index);

    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    ExecutionContext ec = ownerOP.getExecutionContext();
    if (orderMapping != null) {
      ObjectProvider childOP = ec.findObjectProvider(element);
      // See DatastoreFieldManager.handleIndexFields for info on why this absurdity is necessary.
      Entity childEntity =
          (Entity) childOP.getAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED);
      if (childEntity != null) {
          childOP.setAssociatedValue(DatastorePersistenceHandler.ENTITY_WRITE_DELAYED, null);
          childOP.setAssociatedValue(orderMapping, index);
        handler.insertObject(childOP);
      }
    }

    if (ownerMemberMetaData.getCollection().isDependentElement() && allowCascadeDelete && obj != null) {
      ec.deleteObjectInternal(obj);
    }

    // TODO Remove this nonsense
    if (!ec.getTransaction().isActive()) {
      ec.getTransaction().addTransactionEventListener(new ForceFlushPreCommitTransactionEventListener(ownerOP));
    }
    return obj;
  }

  /* (non-Javadoc)
   * @see org.datanucleus.store.scostore.ListStore#subList(org.datanucleus.store.ObjectProvider, int, int)
   */
  public List subList(ObjectProvider ownerOP, int startIdx, int endIdx) {
    ListIterator iter = listIterator(ownerOP, startIdx, endIdx);
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
      String propertyName = getIndexPropertyName();
      // Order by the ordering column
      Query.SortPredicate sortPredicate =
          new Query.SortPredicate(propertyName, Query.SortDirection.ASCENDING);
      sortPredicates.add(sortPredicate);
    } else {
      for (OrderMetaData.FieldOrder fieldOrder : ownerMemberMetaData.getOrderMetaData().getFieldOrders()) {
        String propertyName = fieldOrder.getFieldName();
        boolean isPrimaryKey = isPrimaryKey(propertyName);
        if (isPrimaryKey) {
          if (fieldOrder.isForward() && sortPredicates.isEmpty()) {
            // Don't even bother adding if the first sort is id ASC (this is the
            // default sort so there's no point in making the datastore figure this out).
            break;
          }
          // sorting by id requires us to use a reserved property name
          propertyName = Entity.KEY_RESERVED_PROPERTY;
        }
        Query.SortPredicate sortPredicate = new Query.SortPredicate(
            propertyName, fieldOrder.isForward() ? Query.SortDirection.ASCENDING : Query.SortDirection.DESCENDING);
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
    DatastoreTable table = (DatastoreTable) this.elementTable;
    AbstractMemberMetaData ammd = table.getClassMetaData().getMetaDataForMember(propertyName);
    return ammd.isPrimaryKey();
  }

  /**
   * Method to validate that an element is valid for writing to the datastore.
   * TODO Minimise differences to super.validateElementForWriting()
   * @param ownerOP ObjectProvider for the owner of the List
   * @param element The element to validate
   * @param index The position that the element is being stored at in the list
   * @return Whether the element was inserted
   */
  protected boolean validateElementForWriting(final ObjectProvider ownerOP, Object element, final int index)
  {
    final Object newOwner = ownerOP.getObject();

    // Check if element is ok for use in the datastore, specifying any external mappings that may be required
    boolean inserted = super.validateElementForWriting(ownerOP.getExecutionContext(), element, new FieldValues()
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
            elementOP.setAssociatedValue(externalFKMapping, ownerOP.getObject());
          }

          if (orderMapping != null && index >= 0) {
            if (ownerMemberMetaData.getOrderMetaData() != null && ownerMemberMetaData.getOrderMetaData().getMappedBy() != null) {
              // Order is stored in a field in the element so update it
              // We support mapped-by fields of types int/long/Integer/Long currently
              Object indexValue = null;
              if (orderMapping.getMemberMetaData().getTypeName().equals(ClassNameConstants.JAVA_LANG_LONG) ||
                  orderMapping.getMemberMetaData().getTypeName().equals(ClassNameConstants.LONG)) {
                indexValue = Long.valueOf(index);
              }
              else {
                indexValue = Integer.valueOf(index);
              }
              elementOP.replaceFieldMakeDirty(orderMapping.getMemberMetaData().getAbsoluteFieldNumber(), indexValue);
            }
            else {
              // Order is stored in a surrogate column so save its vaue for the element to use later
              elementOP.setAssociatedValue(orderMapping, Integer.valueOf(index));
            }
          }
        }
        if (Relation.isBidirectional(relationType)) {
          // TODO This is ManagedRelations - move into RelationshipManager
          // Managed Relations : 1-N bidir, so make sure owner is correct at persist
          Object currentOwner = elementOP.provideField(elementMemberMetaData.getAbsoluteFieldNumber());
          if (currentOwner == null) {
            // No owner, so correct it
            NucleusLogger.PERSISTENCE.info(LOCALISER.msg("056037",
                ownerOP.toPrintableID(), ownerMemberMetaData.getFullFieldName(), 
                StringUtils.toJVMIDString(elementOP.getObject())));
            elementOP.replaceFieldMakeDirty(elementMemberMetaData.getAbsoluteFieldNumber(), newOwner);
          }
          else if (currentOwner != newOwner && ownerOP.getReferencedPC() == null) {
            // Owner of the element is neither this container nor is it being attached
            // Inconsistent owner, so throw exception
            throw new NucleusUserException(LOCALISER.msg("056038",
                ownerOP.toPrintableID(), ownerMemberMetaData.getFullFieldName(), 
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