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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.store.mapped.scostore.FKListStore;

import java.util.List;
import java.util.ListIterator;

/**
 * Datastore-specific implementation of an {@link FKListStore}.
 */
public class DatastoreFKListStore extends FKListStore {

  public DatastoreFKListStore(AbstractMemberMetaData fmd, DatastoreManager storeMgr,
      ClassLoaderResolver clr) {
    super(fmd, storeMgr, clr, new DatastoreFKListStoreSpecialization(LOCALISER, clr, storeMgr));
  }

  protected ListIterator listIterator(StateManager ownerSM, int startIdx, int endIdx) {
    ObjectManager om = ownerSM.getObjectManager();
    ApiAdapter apiAdapter = om.getApiAdapter();
    Key parentKey = EntityUtils.getPrimaryKeyAsKey(apiAdapter, ownerSM);
    return ((DatastoreAbstractListStoreSpecialization) specialization).getChildren(
        parentKey,
        getFilterPredicates(startIdx, endIdx),
        getSortPredicates(), this, om).listIterator();
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
      // I'm not sure what we should do if this mapping doesn't exist so for now
      // we'll just blow up.
      propertyName =
          orderMapping.getDataStoreMappings()[0].getDatastoreField().getIdentifier().getIdentifierName();
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
            // default sort so there's no point in making the datastore figure this
            // out).
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
    return ((DatastoreTable) containerTable).getDatastoreField(propertyName).isPrimaryKey();
  }

}