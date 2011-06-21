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
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.fieldmanager.SingleValueFieldManager;

import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

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
   * Set is used to pass messages between child and parent during
   * cascades.  The entity uniquely identified by Any {@link Key}
   * in this set needs to have its relation fields re-persisted.
   */
  private final Set<Key> modifiedParentSet = new HashSet<Key>();

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
    AbstractClassMetaData acmd = stateMgr.getClassMetaData();
    for (AbstractMemberMetaData dependent : dt.getSameEntityGroupMemberMetaData()) {
      // Make sure we only provide the field for the correct part of any inheritance tree
      if (dependent.getAbstractClassMetaData().isSameOrAncestorOf(acmd)) {
        stateMgr.provideFields(new int[]{dependent.getAbsoluteFieldNumber()}, sfv);
        Object childValue = sfv.fetchObjectField(dependent.getAbsoluteFieldNumber());
        if (childValue != null) {
          if (childValue instanceof Object[]) {
            childValue  = Arrays.asList((Object[]) childValue);
          }
          if (childValue instanceof Iterable) {
            // TODO(maxr): Make sure we're not pulling back unnecessary data
            // when we iterate over the values.
            String expectedType = getExpectedChildType(dependent);
            for (Object element : (Iterable) childValue) {
              addToParentKeyMap(element, key, stateMgr, expectedType, true);
            }
          } else {
            boolean checkForPolymorphism = !dt.isParentKeyProvider(dependent);
            addToParentKeyMap(
                childValue, key, stateMgr, getExpectedChildType(dependent), checkForPolymorphism);
          }
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
    
  void registerKey(Object childValue, Key key, StateManager stateMgr, String expectedType) {
    addToParentKeyMap(childValue, key, stateMgr, expectedType, false);
  }

  Key getRegisteredKey(Object object) {
    return parentKeyMap.get(object);
  }

  void clear() {
    parentKeyMap.clear();
  }

  private void addToParentKeyMap(Object childValue, Key key,
                                 StateManager stateMgr, String expectedType,
                                 boolean checkForPolymorphism) {
    if (checkForPolymorphism && childValue != null && !childValue.getClass().getName()
        .equals(expectedType)) {
      AbstractClassMetaData acmd = stateMgr.getMetaDataManager().getMetaDataForClass(
          childValue.getClass(), stateMgr.getObjectManager().getClassLoaderResolver());
      if (!DatastoreManager.isNewOrSuperclassTableInheritanceStrategy(acmd)) {
        // TODO cache the result of this evaluation to improve performance
        boolean isJPA = ((DatastoreManager) stateMgr.getStoreManager()).isJPA();
        throw new UnsupportedOperationException(
            "Received a child of type " + childValue.getClass().getName() + " for a field of type "
            + expectedType
            + ". Unfortunately polymorphism in relationships is only supported for the " +
            (isJPA ? "SINGLE_TABLE" : "superclass-table") + " inheritance mapping strategy.");
      }
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

  void registerModifiedParent(Key key) {
    modifiedParentSet.add(key);
  }

  void clearModifiedParent(Key key) {
    modifiedParentSet.remove(key);
  }

  boolean parentNeedsUpdate(Key key) {
    return modifiedParentSet.contains(key);
  }
}
