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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.Entity;

import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.store.appengine.Utils;

import java.util.LinkedList;
import java.util.List;

import javax.jdo.spi.PersistenceCapable;

/**
 * Wraps the provided entity-to-pojo {@link org.datanucleus.store.appengine.Utils.Function} in a {@link org.datanucleus.store.appengine.Utils.Function}
 * that extracts the fields identified by the provided field meta-data.
 * @author Max Ross <maxr@google.com>
 */
class ProjectionResultTransformer implements Utils.Function<Entity, Object> {

  private final Utils.Function<Entity, Object> entityToPojoFunc;
  private final ObjectManager objectManager;
  private final Iterable<AbstractMemberMetaData> projectionFields;
  private final String queryString;

  ProjectionResultTransformer(Utils.Function<Entity, Object> entityToPojoFunc,
                              ObjectManager objectManager,
                              Iterable<AbstractMemberMetaData> projectionFields,
                              String queryString) {
    this.entityToPojoFunc = entityToPojoFunc;
    this.objectManager = objectManager;
    this.projectionFields = projectionFields;
    this.queryString = queryString;
  }

  public Object apply(Entity from) {
    PersistenceCapable pc = (PersistenceCapable) entityToPojoFunc.apply(from);
    StateManager sm = objectManager.findStateManager(pc);
    List<Object> values = Utils.newArrayList();
    // Need to fetch the fields one at a time instead of using
    // sm.provideFields() because that method doesn't respect the ordering
    // of the field numbers and that ordering is important here.
    for (AbstractMemberMetaData ammd : projectionFields) {
      AbstractMemberMetaData curMetaData = ammd;
      LinkedList<String> embeddedFieldNames = Utils.newLinkedList();
      // keep walking up the metadata graph until we hit something that is not
      // embedded, collecting field names as we go
      while (curMetaData.getParent() instanceof EmbeddedMetaData) {
        if (!(curMetaData.getParent().getParent() instanceof AbstractMemberMetaData)) {
          throw new DatastoreQuery.UnsupportedDatastoreFeatureException(
              "Unexpected metadata while parsing result expression with embedded fields: "
                  + curMetaData, queryString);
        }
        embeddedFieldNames.add(curMetaData.getName());
        curMetaData = (AbstractMemberMetaData) curMetaData.getParent().getParent();
      }
      // fetch the top level object
      Object curValue = sm.provideField(curMetaData.getAbsoluteFieldNumber());
      // now walk back down, fetching nested field values
      while (!embeddedFieldNames.isEmpty()) {
        StateManager embeddedSm = objectManager.findStateManager(curValue);
        AbstractClassMetaData embeddedClassMetaData = embeddedSm.getClassMetaData();
        AbstractMemberMetaData embeddedField = embeddedClassMetaData.getMetaDataForMember(embeddedFieldNames.removeFirst());
        curValue = embeddedSm.provideField(embeddedField.getAbsoluteFieldNumber());
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
}
