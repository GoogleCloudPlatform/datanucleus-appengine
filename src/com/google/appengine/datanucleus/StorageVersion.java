/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus;

import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.store.StoreManager;

import java.util.Arrays;

/**
 * The storage versions we support.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public enum StorageVersion {

  /**
   * This is the original storage version (GAE DataNucleus plugin v1) and represents how we stored
   * owned relationships before we had a concrete notion of storage version.
   * Originally the datastore only supported one write per entity per txn,
   * which meant there was no way to add a child key to a parent inside a txn:
   * Write the parent to get the parent key, write the child passing the parent
   * key as the parent, update the parent with the child key - boom.  To work
   * around this we just did ancestor queries and then filtered by depth
   * in-memory to resolve owned relationships (1-1 and 1-N).
   */
  PARENTS_DO_NOT_REFER_TO_CHILDREN,

  /**
   * Now that the datastore supports multiple writes per entity
   * per txn we can store child keys on the parent entity.  Apps that run
   * without any storage version specified in their config operate in this
   * mode.  This allows apps to start writing data in the new storage format
   * even if they haven't migrated existing data to use the new format.  This
   * storage version is backwards compatible with
   * {@link #PARENTS_DO_NOT_REFER_TO_CHILDREN}.
   */
  WRITE_OWNED_CHILD_KEYS_TO_PARENTS,

  /**
   * Storage version where we start to take advantage of the child keys on the
   * parent entity.  Users should not switch to this storage version until all
   * parent entities have been updated with references to all children.  This
   * storage version is backwards compatible with
   * {@link #WRITE_OWNED_CHILD_KEYS_TO_PARENTS}.
   */
  READ_OWNED_CHILD_KEYS_FROM_PARENTS;

  /**
   * Config property that determines the action we take when we encounter
   * ignorable meta-data.
   */
  public static final String STORAGE_VERSION_PROPERTY = "datanucleus.appengine.storageVersion";

  /**
   * The default storage version.  If {@link #STORAGE_VERSION_PROPERTY} is not
   * defined in the config, this is the storage version we use.
   */
  private static final StorageVersion DEFAULT = READ_OWNED_CHILD_KEYS_FROM_PARENTS;

  static StorageVersion fromStoreManager(StoreManager storeMgr) {
    String val = storeMgr.getStringProperty(STORAGE_VERSION_PROPERTY);
    // if the user hasn't specific a specific storage version we'll use the default
    if (val == null) {
      return DEFAULT;
    }
    try {
      return StorageVersion.valueOf(val);
    } catch (IllegalArgumentException iae) {
      throw new NucleusFatalUserException(
          String.format("'%s' is an unknwon value for %s.  Legal values are %s.",
                        val, STORAGE_VERSION_PROPERTY, Arrays.toString(StorageVersion.values())));
    }
  }
}
