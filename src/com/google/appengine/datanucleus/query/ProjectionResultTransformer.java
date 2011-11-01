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
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;

import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

import org.datanucleus.query.QueryUtils;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.mapping.EmbeddedMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.spi.PersistenceCapable;

/**
 * Wraps the provided entity-to-pojo {@link Utils.Function} in a  {@link Utils.Function}
 * that extracts the fields identified by the provided field meta-data.
 * @author Max Ross <maxr@google.com>
 */
class ProjectionResultTransformer implements Utils.Function<Entity, Object> {

  protected static final Localiser LOCALISER=Localiser.getInstance(
      "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

  private final Utils.Function<Entity, Object> entityToPojoFunc;
  private final ExecutionContext ec;
  private final List<String> projectionFields;
  private final List<String> projectionAliases;
  private final String candidateAlias;
  private final Class resultClass;
  private final Map<String, Field> resultClassFieldsByName = new HashMap();

  ProjectionResultTransformer(Utils.Function<Entity, Object> entityToPojoFunc, ExecutionContext ec,
      String candidateAlias, Class resultClass, List<String> projectionFields, List<String> projectionAliases) {
    this.entityToPojoFunc = entityToPojoFunc;
    this.ec = ec;
    this.projectionFields = projectionFields;
    this.projectionAliases = projectionAliases;
    this.candidateAlias = candidateAlias;
    this.resultClass = resultClass;
    if (resultClass != null && !QueryUtils.resultClassIsSimple(resultClass.getName())) {
      populateDeclaredFieldsForUserType(resultClass, resultClassFieldsByName);
    }
  }

  /**
   * Method to convert the Entity for this row into the required query result.
   * Processes any result expression(s), and any result class.
   * @param from The entity
   * @return The required result format, for this Entity
   */
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
      List<String> fieldNames = getTuples(projectionField, candidateAlias);
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

    if (resultClass != null) {
      // Convert field values into result object
      Object[] valueArray = values.toArray(new Object[values.size()]);
      return getResultObjectForValues(valueArray);
    }

    if (values.size() == 1) {
      // If there's only one value, just return it.
      return values.get(0);
    }
    // Return an Object array.
    return values.toArray(new Object[values.size()]);
  }

  private Object getResultObjectForValues(Object[] values) {

    if (QueryUtils.resultClassIsSimple(resultClass.getName())) {
      // User wants a single field
      if (values.length == 1 && (values[0] == null || resultClass.isAssignableFrom(values[0].getClass()))) {
        // Simple object is the correct type so just give them the field
        return values[0];
      }
      else if (values.length == 1 && !resultClass.isAssignableFrom(values[0].getClass())) {
        // Simple object is not assignable to the ResultClass so throw an error
        String msg = LOCALISER.msg("021202",
            resultClass.getName(), values[0].getClass().getName());
        NucleusLogger.QUERY.error(msg);
        throw new NucleusUserException(msg);
      }
      else {
        throw new NucleusUserException("Result class is simple, but field value " + values + " not convertible into that");
      }
    } else {
      // User requires creation of one of his own type of objects, or a Map
      // A. Find a constructor with the correct constructor arguments
      Class[] resultFieldTypes = new Class[values.length];
      for (int i=0;i<values.length;i++) {
        resultFieldTypes[i] = values[i].getClass(); // TODO Cater for null (need passing in from field info)
      }
      Object obj = QueryUtils.createResultObjectUsingArgumentedConstructor(resultClass, values, resultFieldTypes);
      if (obj != null) {
        return obj;
      }

      // B. No argumented constructor exists so create an object and update fields using fields/put method/set method
      String[] resultFieldNames = projectionAliases.toArray(new String[projectionAliases.size()]);
      obj = QueryUtils.createResultObjectUsingDefaultConstructorAndSetters(resultClass, resultFieldNames, 
          resultClassFieldsByName, values);

      return obj;
    }
  }

  /**
   * Populate a map with the declared fields of the result class and super classes.
   * @param cls the class to find the declared fields and populate the map
   */
  private void populateDeclaredFieldsForUserType(Class cls, Map resultClassFieldsByName)
  {
      for (int i=0;i<cls.getDeclaredFields().length;i++)
      {
          if (resultClassFieldsByName.put(cls.getDeclaredFields()[i].getName().toUpperCase(), cls.getDeclaredFields()[i]) != null)
          {
              throw new NucleusUserException(LOCALISER.msg("021210", cls.getDeclaredFields()[i].getName()));
          }
      }
      if (cls.getSuperclass() != null)
      {
          populateDeclaredFieldsForUserType(cls.getSuperclass(), resultClassFieldsByName);
      }
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
