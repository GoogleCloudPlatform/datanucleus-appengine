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
package com.google.appengine.datanucleus.query;

import com.google.appengine.api.datastore.Entity;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractMemberMetaData;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.mapping.EmbeddedMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

import java.util.Arrays;
import java.util.List;

import javax.jdo.spi.PersistenceCapable;

/**
 * Wraps the provided entity-to-pojo {@link Utils.Function} in a  {@link Utils.Function}
 * that extracts the fields identified by the provided field meta-data.
 * @author Max Ross <maxr@google.com>
 */
class ProjectionResultTransformer implements Utils.Function<Entity, Object> {

  private final Utils.Function<Entity, Object> entityToPojoFunc;
  private final ExecutionContext ec;
  private final List<String> projectionFields;
  private final String alias;

  ProjectionResultTransformer(Utils.Function<Entity, Object> entityToPojoFunc,
                              ExecutionContext ec,
                              List<String> projectionFields,
                              String alias) {
    this.entityToPojoFunc = entityToPojoFunc;
    this.ec = ec;
    this.projectionFields = projectionFields;
    this.alias = alias;
  }

  public Object apply(Entity from) {
    PersistenceCapable pc = (PersistenceCapable) entityToPojoFunc.apply(from);
    ObjectProvider op = ec.findObjectProvider(pc);
    List<Object> values = Utils.newArrayList();
    // Need to fetch the fields one at a time instead of using
    // sm.provideFields() because that method doesn't respect the ordering
    // of the field numbers and that ordering is important here.
    for (String projectionField : projectionFields) {
      ObjectProvider currentOP = op;
      DatastoreManager storeMgr = (DatastoreManager) ec.getStoreManager();
      ClassLoaderResolver clr = ec.getClassLoaderResolver();
      List<String> fieldNames = getTuples(projectionField, alias);
      JavaTypeMapping typeMapping;
      Object curValue = null;
      boolean shouldBeDone = false;
      for (String fieldName : fieldNames) {
        if (shouldBeDone) {
          throw new RuntimeException(
              "Unable to extract field " + projectionField + " from " +
              op.getClassMetaData().getFullClassName() + ".  This is most likely an App Engine bug.");
        }
        DatastoreTable table = storeMgr.getDatastoreClass(
            currentOP.getClassMetaData().getFullClassName(), clr);
        typeMapping = table.getMappingForSimpleFieldName(fieldName);
        if (typeMapping instanceof EmbeddedMapping) {
          // reset the mapping to be the mapping for the next embedded field
          typeMapping = table.getMappingForSimpleFieldName(fieldName);
        } else {
          // The first non-embedded mapping should be for the field
          // with the value we ultimately want to return.
          // If we still have more tuples then that's an error.
          shouldBeDone = true;
        }
        AbstractMemberMetaData curMemberMetaData = typeMapping.getMemberMetaData();
        curValue = currentOP.provideField(curMemberMetaData.getAbsoluteFieldNumber());
        if (curValue == null) {
          // If we hit a null value we're done even if we haven't consumed
          // all the tuple fields
          break;
        }
        currentOP = ec.findObjectProvider(curValue);
      }
      // the value we have left at the end is the embedded field value we want to return
      values.add(curValue);
    }
    if (values.size() == 1) {
      // If there's only one value, just return it.
      return values.get(0);
    }
    // Return an Object array.
    return values.toArray(new Object[values.size()]);
  }

  /**
   * Turns "a.b.c into a List containing "a", "b", and "c"
   */
  private static List<String> getTuples(String dotDelimitedFieldName, String alias) {
    // split takes a regex so need to escape the period character
    List<String> tuples = Arrays.asList(dotDelimitedFieldName.split("\\."));
    return DatastoreQuery.getTuples(tuples, alias);
  }

}
