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

import com.google.appengine.api.datastore.Key;

import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;
import org.datanucleus.store.mapped.MappedStoreManager;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * A registry for the keys of objects that have been inserted as part of a
 * transaction.
 *
 * @author Max Ross <maxr@google.com>
 */
class KeyRegistry {

  /**
   * Map is used to pass messages between parent and child
   * during cascades.  We use an IdentityHashMap here
   * because we really really (really) want reference equality, not object
   * equality.
   */
  private final Map<Object, Key> parentKeyMap = new IdentityHashMap<Object, Key>();

  /**
   * In order to automatically assign children to the same entity groups as
   * their parents, we need to make sure the Key of the parent entity is
   * available to the children when their corresponding entities are created.
   * We do this by looping over all the dependent fields of the parent, and for
   * each dependent field we extract the value of that field from the parent.
   * We then add that field value as the key in an identity map, associating
   * it with the Key of the parent entity.  Anytime we insert a new object
   * we need to consult this map to see if a parent Key has been mapped to it,
   * and if it has we use that Key as the parent key for the new Entity we
   * are creating.
   */
  void registerKey(MappedStoreManager storeMgr, StateManager stateMgr,
      DatastoreFieldManager fieldMgr) {
    DatastoreTable dt = (DatastoreTable) storeMgr.getDatastoreClass(
        fieldMgr.getClassMetaData().getFullClassName(), fieldMgr.getClassLoaderResolver());
    SingleValueFieldManager sfv = new SingleValueFieldManager();
    Key key = fieldMgr.getEntity().getKey();
    for (AbstractMemberMetaData dependent : dt.getSameEntityGroupMemberMetaData()) {
      stateMgr.provideFields(new int[]{dependent.getAbsoluteFieldNumber()}, sfv);
      Object childValue = sfv.fetchObjectField(dependent.getAbsoluteFieldNumber());
      if (childValue != null) {
        if (childValue instanceof Object[]) {
          childValue  = Arrays.asList((Object[]) childValue);
        }
        if (childValue instanceof Iterable) {
          // TODO(maxr): Make sure we're not pulling back unnecessary data
          // when we iterate over the values.
          for (Object element : (Iterable) childValue) {
            parentKeyMap.put(element, key);
          }
        } else {
          parentKeyMap.put(childValue, key);
        }
      }
    }
  }

  void registerKey(Object childValue, Key key) {
    parentKeyMap.put(childValue, key);  
  }

  Key getRegisteredKey(Object object) {
    return parentKeyMap.get(object);
  }

  void clear() {
    parentKeyMap.clear();
  }

  /**
   * Get the {@link KeyRegistry} associated with the current datasource
   * connection.  There's a little bit of fancy footwork involved here
   * because, by default, asking the storeManager for a connection will
   * allocate a transactional connection if no connection has already been
   * established.  That's acceptable behavior if the datasource has not been
   * configured to allow writes outside of transactions, but if the datsaource
   * _has_ been configured to allow writes outside of transactions,
   * establishing a transaction is not the right thing to do.  So, we set
   * a property on the currently active transaction (the datanucleus
   * transaction, not the datastore transaction) to indicate that if a
   * connection gets allocated, don't establish a datastore transaction.
   * Note that even if nontransactional writes are enabled, if there
   * is already a connection available then setting the property is a no-op.
   */
  static KeyRegistry getKeyRegistry(ObjectManager om) {
    StoreManager storeManager = om.getStoreManager();
    ManagedConnection mconn = storeManager.getConnection(om);
    return ((EmulatedXAResource) mconn.getXAResource()).getKeyRegistry();
  }
}
