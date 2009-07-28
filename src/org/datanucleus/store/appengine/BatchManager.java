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

import java.util.ArrayList;
import java.util.List;

/**
 * Manages batch operations that are initiated via standard JDO and JPA
 * calls.
 *
 * @author Max Ross <maxr@google.com>
 */
public abstract class BatchManager<T> {

  /**
   * We don't support nested batch operations.
   */
  private final ThreadLocal<List<T>> batchStateList = new ThreadLocal<List<T>>();

  boolean batchOperationInProgress() {
    return batchStateList.get() != null;
  }

  public void start() {
    if (batchOperationInProgress()) {
      throw new IllegalStateException("Batch " + getOperation() + " already running.");
    }
    batchStateList.set(new ArrayList<T>());
  }
  
  public void finish(DatastorePersistenceHandler handler) {
    if (!batchOperationInProgress()) {
      throw new IllegalStateException("Batch " + getOperation() + " not running.");
    }
    List<T> processMe = batchStateList.get();
    // We want to make sure the callbacks are emptied out no matter what
    // happens when we try to insert.
    batchStateList.remove();
    if (!processMe.isEmpty()) {
      processBatchState(handler, processMe);
    }
  }

  void add(T batchState) {
    batchStateList.get().add(batchState);
  }

  abstract String getOperation();
  abstract void processBatchState(DatastorePersistenceHandler handler, List<T> batchStateList);
}
