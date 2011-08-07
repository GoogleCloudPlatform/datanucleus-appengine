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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;

import org.datanucleus.store.ObjectProvider;

/**
 * A {@link DatastoreFieldManager} extension that ignores attempts to
 * write to the parent field.  Should only be used for processing deletes.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreDeleteFieldManager extends StoreFieldManager {

  DatastoreDeleteFieldManager(ObjectProvider op, DatastoreManager storeManager, Entity datastoreEntity) {
    super(op, datastoreEntity, Operation.DELETE);
  }

  @Override
  void storeParentField(int fieldNumber, Object value) {
    // swallow it - we typically treat this as an error but if it's part of a
    // delete operation we don't care
  }

  @Override
  void storePKIdField(int fieldNumber, Object value) {
    // swallow it - we typically treat this as an error but if it's part of a
    // delete operation we don't care
  }
}