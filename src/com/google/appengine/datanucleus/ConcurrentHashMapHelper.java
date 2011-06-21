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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper methods for manipulating a {@link ConcurrentHashMap}.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class ConcurrentHashMapHelper {

  /**
   * Fetch the current value associated with the provided key, establishing a
   * value initialized to 0 in a threadsafe way if no current value exists.
   */
  public static <K> AtomicInteger getCounter(ConcurrentHashMap<K, AtomicInteger> map, K key) {
    AtomicInteger count = map.get(key);
    if (count == null) {
      // we don't want to overwrite a value that was added in between the
      // fetch and the null check, so only add this value if the key is
      // not already associated with a value.
      map.putIfAbsent(key, new AtomicInteger(0));
      // the result will either be the value we just put or the value that
      // was inserted by another thread after our null-check.  Either way,
      // we're guaranteed to have the latest.
      count = map.get(key);
    }
    return count;
  }

  private ConcurrentHashMapHelper() {}
}
