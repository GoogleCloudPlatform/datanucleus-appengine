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
package com.google.appengine.datanucleus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.util.CommandLine;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.SortPredicate;

/**
 * Migration tool for moving from GAE v1 StorageVersion (children identified by parent key) to
 * GAE v2 StorageVersion (child keys stored in parents).
 */
public class Migrator {

  public static void main(String[] args) {
    CommandLine cmd = new CommandLine();
    cmd.addOption("pu", "persistenceUnit", "<persistence-unit>", 
      "name of the persistence unit to handle the schema for");
    cmd.addOption("pmf", "pmfName", "<pmf-name>",
      "name of the PMF to handle the schema for");
    cmd.parse(args);

    // Remaining command line args are filenames (class files, metadata files)
    String[] filenames = cmd.getDefaultArgs();

    String persistenceUnitName = null;
    if (cmd.hasOption("pu"))
    {
        persistenceUnitName = cmd.getOptionArg("pu");
    }

    // Initialise the context for this API
    NucleusContext nucleusCtx = new NucleusContext("JDO", null);
    PersistenceConfiguration propConfig = nucleusCtx.getPersistenceConfiguration();

    Properties props = new Properties();
    props.setProperty("datanucleus.ConnectionURL", "appengine");
    propConfig.setPersistenceProperties(props);

    System.out.println("GAE Migrator for persistence-unit=" + persistenceUnitName + " migrating the following classes : " +
        StringUtils.objectArrayToString(filenames));
    // TODO Identify classes to be migrated and pass through the method below
  }

  /**
   * Method to migrate all entities for the specified class to having child keys stored in the parent.
   * @param className Name of the class (fully qualified)
   * @param nucCtx Nucleus Context
   */
  public static void migrateClass(String className, NucleusContext nucCtx) {
    DatastoreManager storeMgr = (DatastoreManager) nucCtx.getStoreManager();
    ClassLoaderResolver clr = nucCtx.getClassLoaderResolver(null);
    AbstractClassMetaData cmd = nucCtx.getMetaDataManager().getMetaDataForClass(className, clr);
    NucleusLogger.GENERAL.info(">> Migrating Entities for " + className);

    Set<Entity> changedEntities = new HashSet<Entity>();
    int[] relationFieldNumbers = cmd.getRelationMemberPositions(clr, nucCtx.getMetaDataManager());
    if (relationFieldNumbers != null && relationFieldNumbers.length > 0) {
      DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

      // Do a query for the particular kind, restricting it by any discriminator
      String kindName = EntityUtils.getKindName(storeMgr.getIdentifierFactory(), cmd);

      // Query for all Entities of this class
      Query q = new Query(kindName);
      if (cmd.hasDiscriminatorStrategy()) {
        String discriminatorPropertyName = EntityUtils.getDiscriminatorPropertyName(storeMgr.getIdentifierFactory(), 
            cmd.getDiscriminatorMetaDataRoot());
        Object discrimValue = cmd.getDiscriminatorValue();
        q.addFilter(discriminatorPropertyName, Query.FilterOperator.EQUAL, discrimValue);
      }

      PreparedQuery pq = datastore.prepare(q);
      for (Entity entity : pq.asIterable()) {
        // For each Entity, process the owner fields
        NucleusLogger.GENERAL.info(">> Migrating Entity with key=" + entity);
        for (int i=0;i<relationFieldNumbers.length;i++) {
          AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(relationFieldNumbers[i]);
          if (MetaDataUtils.isOwnedRelation(mmd)) {
            int relationType = mmd.getRelationType(clr);
            if (relationType == Relation.ONE_TO_ONE_UNI ||
                (relationType == Relation.ONE_TO_ONE_BI && mmd.getMappedBy() == null)) {
              // 1-1 owner
              String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), mmd);
              if (!entity.hasProperty(propName)) {
                // Add property in Entity with child key
                AbstractClassMetaData relCmd = nucCtx.getMetaDataManager().getMetaDataForClass(mmd.getTypeName(), clr);
                String relKindName = EntityUtils.getKindName(storeMgr.getIdentifierFactory(), relCmd);
                Query q2 = new Query(relKindName, entity.getKey());
                PreparedQuery pq2 = datastore.prepare(q2);
                Object value = null;
                for (Entity childEntity : pq2.asIterable()) { // Should be only one (see FetchFieldManager)
                  if (entity.getKey().equals(childEntity.getKey().getParent())) {
                    value = childEntity.getKey();
                    break;
                  }
                }
                entity.setProperty(propName, value);
              }
            }
            else if (relationType == Relation.ONE_TO_MANY_UNI || relationType == Relation.ONE_TO_MANY_BI) {
              // 1-N
              String propName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), mmd);
              if (!entity.hasProperty(propName)) {
                AbstractClassMetaData childCmd = mmd.getCollection().getElementClassMetaData(clr, nucCtx.getMetaDataManager());
                String childKindName = EntityUtils.getKindName(storeMgr.getIdentifierFactory(), childCmd);
                Query q2 = new Query(childKindName, entity.getKey());

                if (List.class.isAssignableFrom(mmd.getType())) {
                  // Add List ordering
                  List<Query.SortPredicate> sortPredicates = Utils.newArrayList();
                  boolean indexedList = true;
                  if (mmd.getOrderMetaData() != null && !mmd.getOrderMetaData().isIndexedList()) {
                    indexedList = false;
                  }
                  if (indexedList) {
                    // Get the index property name
                    String indexPropName = null;
                    OrderMetaData ordmd = mmd.getOrderMetaData();
                    if (ordmd != null) {
                      String colName;
                      if (ordmd.getColumnMetaData() != null && ordmd.getColumnMetaData().length > 0
                          && ordmd.getColumnMetaData()[0].getName() != null) {
                        // User-defined name so create an identifier using it
                        colName = ordmd.getColumnMetaData()[0].getName();
                        indexPropName = storeMgr.getIdentifierFactory().newDatastoreFieldIdentifier(colName).getIdentifierName();
                      }
                    }
                    if (indexPropName == null) {
                      // No name defined so generate one
                      indexPropName = storeMgr.getIdentifierFactory().newForeignKeyFieldIdentifier(
                          mmd, null, null, true, FieldRole.ROLE_INDEX).getIdentifierName();
                    }

                    // Order by the ordering column
                    Query.SortPredicate sortPredicate =
                      new Query.SortPredicate(indexPropName, Query.SortDirection.ASCENDING);
                    sortPredicates.add(sortPredicate);
                  } else {
                    for (OrderMetaData.FieldOrder fieldOrder : mmd.getOrderMetaData().getFieldOrders()) {
                      String orderMemberName = fieldOrder.getFieldName();
                      AbstractMemberMetaData orderMemberMmd = childCmd.getMetaDataForMember(orderMemberName);
                      String orderPropName = EntityUtils.getPropertyName(storeMgr.getIdentifierFactory(), orderMemberMmd);
                      boolean isPrimaryKey = orderMemberMmd.isPrimaryKey();
                      if (isPrimaryKey) {
                        if (fieldOrder.isForward() && sortPredicates.isEmpty()) {
                          // Don't even bother adding if the first sort is id ASC (this is the
                          // default sort so there's no point in making the datastore figure this out).
                          break;
                        }
                        // sorting by id requires us to use a reserved property name
                        orderPropName = Entity.KEY_RESERVED_PROPERTY;
                      }
                      Query.SortPredicate sortPredicate = new Query.SortPredicate(orderPropName,
                          fieldOrder.isForward() ? Query.SortDirection.ASCENDING : Query.SortDirection.DESCENDING);
                      sortPredicates.add(sortPredicate);
                      if (isPrimaryKey) {
                        // User wants to sort by pk.  Since pk is guaranteed to be unique, break
                        // because we know there's no point in adding any more sort predicates
                        break;
                      }
                    }
                  }
                  for (SortPredicate sp : sortPredicates) {
                    q.addSort(sp.getPropertyName(), sp.getDirection());
                  }
                }

                PreparedQuery pq2 = datastore.prepare(q2);
                List<Key> values = new ArrayList<Key>();
                for (Entity childEntity : pq2.asIterable()) { // Should be only one (see FetchFieldManager)
                  if (entity.getKey().equals(childEntity.getKey().getParent())) {
                    values.add(childEntity.getKey());
                  }
                }

                entity.setProperty(propName, values);
              }
            }
          }
        }
      }
      
      if (!changedEntities.isEmpty()) {
        // PUT the updated entities
        NucleusLogger.DATASTORE_NATIVE.debug("Putting " + changedEntities.size() + " entities of kind " + kindName);
        datastore.put(changedEntities);
      }
    }
  }
}
