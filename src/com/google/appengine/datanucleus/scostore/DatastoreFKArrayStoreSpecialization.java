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
package com.google.appengine.datanucleus.scostore;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastorePersistenceHandler;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.store.mapped.scostore.FKArrayStoreSpecialization;
import org.datanucleus.util.Localiser;

/**
 * Datastore-specific implementation of {@link FKArrayStoreSpecialization}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFKArrayStoreSpecialization extends DatastoreAbstractArrayStoreSpecialization
    implements FKArrayStoreSpecialization {

  public DatastoreFKArrayStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
      DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public boolean getUpdateElementFk(ObjectProvider op, Object element, Object owner, int index,
      ElementContainerStore ecs) {
    JavaTypeMapping orderMapping = ecs.getOrderMapping();
    if (orderMapping != null) {
      DatastorePersistenceHandler handler = storeMgr.getPersistenceHandler();
      ExecutionContext ec = op.getExecutionContext();
      ObjectProvider childOP = ec.findObjectProvider(element);
      Entity childEntity = handler.getAssociatedEntityForCurrentTransaction(childOP);
      orderMapping.setObject(ec, childEntity, new int[1], index);
      handler.put(ec, childOP.getClassMetaData(), childEntity);
      return true;
    }
    return false;
  }
}
