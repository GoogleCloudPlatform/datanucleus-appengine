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
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.AbstractListStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.util.Localiser;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Datastore-specific implementation of {@link AbstractListStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
abstract class DatastoreAbstractListStoreSpecialization extends DatastoreAbstractCollectionStoreSpecialization
  implements AbstractListStoreSpecialization {

  DatastoreAbstractListStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
                                           DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public int indexOf(ObjectProvider parentOP, Object element, ElementContainerStore ecs) {
    ObjectProvider elementOP = parentOP.getExecutionContext().findObjectProvider(element);
    Key elementKey = EntityUtils.getPrimaryKeyAsKey(elementOP.getExecutionContext().getApiAdapter(), elementOP);
    if (elementKey == null) {
      throw new NucleusUserException("Collection element does not have a primary key.");
    } else if (elementKey.getParent() == null) {
      throw new NucleusUserException("Collection element primary key does not have a parent.");
    }

    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
    DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
    try {
      Entity e = service.get(elementKey);
      return extractIndexProperty(e, ecs, elementOP.getExecutionContext());
    } catch (EntityNotFoundException enfe) {
      throw new NucleusDataStoreException("Could not determine index of entity.", enfe);
    }
  }

  public int lastIndexOf(ObjectProvider op, Object element, ElementContainerStore ecs) {
    // TODO(maxr) Only seems to be called when useCache on the List
    // is false, but it's true in all my tests and it looks like you
    // need to set datanucleus-specific properties to get it to be false.
    // See SCOUtils#useContainerCache.  We'll take care of this later.
    throw new UnsupportedOperationException();
  }

  public int[] getIndicesOf(ObjectProvider op, Collection elements, ElementContainerStore ecs) {
    // invoked when List.removeAll() is called.
    // Since the datastore doesn't support 'or' we're going to sort the keys
    // in memory, issue an ancestor query that fetches all children between
    // the first key and the last, and then build the array of indices from there.
    // The query may return entities that are not in the elements so we have to
    // be careful.
    if (elements.isEmpty()) {
      return new int[0];
    }
    List<Key> keys = Utils.newArrayList();
    Set<Key> keySet = Utils.newHashSet();
    for (Object ele : elements) {
      ApiAdapter apiAdapter = op.getExecutionContext().getApiAdapter();
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
    AbstractClassMetaData emd = ecs.getEmd();
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
        indices[index++] = extractIndexProperty(e, ecs, op.getExecutionContext());
      }
    }
    if (index != indices.length) {
      // something was missing in the result set
      throw new NucleusDataStoreException("Too few keys returned.");
    }
    return indices;
  }

  private int extractIndexProperty(Entity e, ElementContainerStore ecs, ExecutionContext ec) {
    JavaTypeMapping orderMapping = ecs.getOrderMapping();
    Long indexVal = (Long) orderMapping.getObject(ec, e, new int[1]);
    if (indexVal == null) {
      throw new NucleusDataStoreException("Null index value");
    }
    return indexVal.intValue();
  }

  public int[] internalShift(ObjectProvider ownerOP, ManagedConnection conn, boolean batched,
      int oldIndex, int amount, boolean executeNow,
      ElementContainerStore ecs) throws MappedDatastoreException {
    JavaTypeMapping orderMapping = ecs.getOrderMapping();
    if (orderMapping == null) {
      return null;
    }
    DatastoreServiceConfig config = storeMgr.getDefaultDatastoreServiceConfigForReads();
    DatastoreService service = DatastoreServiceFactoryInternal.getDatastoreService(config);
    AbstractClassMetaData acmd = ecs.getEmd();
    String kind =
        storeMgr.getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
    Query q = new Query(kind);
    ExecutionContext ec = ownerOP.getExecutionContext();
    Object id = ec.getApiAdapter().getTargetKeyForSingleFieldIdentity(
        ownerOP.getInternalObjectId());
    Key key = id instanceof Key ? (Key) id : KeyFactory.stringToKey((String) id);
    q.setAncestor(key);
    // create an entity just to capture the name of the index property
    Entity entity = new Entity(kind);
    orderMapping.setObject(ec, entity, new int[] {1}, oldIndex);
    String indexProp = entity.getProperties().keySet().iterator().next();
    q.addFilter(indexProp, Query.FilterOperator.GREATER_THAN_OR_EQUAL, oldIndex);
    DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
    for (Entity shiftMe : service.prepare(service.getCurrentTransaction(null), q).asIterable()) {
      Long pos = (Long) shiftMe.getProperty(indexProp);
      shiftMe.setProperty(indexProp, pos + amount);
      handler.put(ec, acmd, shiftMe);
    }
    return null;
  }
}
