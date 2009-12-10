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
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;

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
  void registerKey(StateManager stateMgr,
      DatastoreFieldManager fieldMgr) {
    DatastoreTable dt = fieldMgr.getDatastoreTable();
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
            addToParentKeyMap(element, key, getExpectedChildType(dependent), true);
          }
        } else {
          boolean checkForPolymorphism = !dt.isParentKeyProvider(dependent);
          addToParentKeyMap(
              childValue, key, getExpectedChildType(dependent), checkForPolymorphism);
        }
      }
    }
  }

  private String getExpectedChildType(AbstractMemberMetaData dependent) {
    if (dependent.getCollection() != null) {
      CollectionMetaData cmd = dependent.getCollection();
      return cmd.getElementType();
    } else if (dependent.getArray() != null) {
      ArrayMetaData amd = dependent.getArray();
      return amd.getElementType();
    }
    return dependent.getTypeName();
  }

  void registerKey(Object childValue, Key key, String expectedType) {
    addToParentKeyMap(childValue, key, expectedType, false);
  }

  Key getRegisteredKey(Object object) {
    return parentKeyMap.get(object);
  }

  void clear() {
    parentKeyMap.clear();
  }

  private void addToParentKeyMap(Object childValue, Key key, String expectedType, boolean checkForPolymorphism) {
    if (checkForPolymorphism && childValue != null && !childValue.getClass().getName().equals(expectedType)) {
      throw new UnsupportedOperationException(
          "Received a child of type " + childValue.getClass().getName() + " for a field of type "
          + expectedType + ".  Unfortunately polymorphism in relationships is not yet supported.");
    }
    parentKeyMap.put(childValue, key);
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
