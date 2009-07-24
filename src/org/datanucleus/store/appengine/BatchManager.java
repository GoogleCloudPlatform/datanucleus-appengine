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

import org.datanucleus.StateManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages batch puts and deletes that are initiated via standard JDO and JPA
 * calls.
 *
 * @author Max Ross <maxr@google.com>
 */
public class BatchManager {

  /**
   * We don't support nested batch operations.
   */
  private final ThreadLocal<List<StateManager>> callbacks = new ThreadLocal<List<StateManager>>();

  boolean isBatchOperation() {
    return callbacks.get() != null;
  }

  public void startBatchOperation() {
    if (isBatchOperation()) {
      throw new IllegalStateException("Batch operation already running.");
    }
    callbacks.set(new ArrayList<StateManager>());
  }
  
  public void finishBatchOperation(DatastorePersistenceHandler handler) {
    if (!isBatchOperation()) {
      throw new IllegalStateException("Batch operation not running.");
    }
    List<StateManager> runMe = callbacks.get();
    // We want to make sure the callbacks are emptied out no matter what
    // happens when we try to insert.
    callbacks.remove();
    if (!runMe.isEmpty()) {
      handler.insertObjects(runMe);
    }
  }

  void addInsertion(StateManager sm) {
    callbacks.get().add(sm);
  }
}
