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

import java.util.Iterator;

/**
 * Produces {@link Iterator Iterators} that respect the provided
 * offset and limit.
 *
 * @author Max Ross <maxr@google.com>
 */
class SlicingIterable<T> implements Iterable<T> {

  private final int offset;
  private final Integer limit;
  private final Iterable<T> data;

  SlicingIterable(int offset, Integer limit, Iterable<T> data) {
    this.offset = offset;
    this.limit = limit;
    this.data = data;
  }

  public Iterator<T> iterator() {
    return new AbstractIterator<T>() {
      private int remainingOffset = offset;
      private Integer numEntitiesReturned = 0;
      private final Iterator<T> iter = data.iterator();

      protected T computeNext() {
        if (numEntitiesReturned.equals(limit)) {
          endOfData();
        }
        T next = null;
        while (iter.hasNext() && remainingOffset-- >= 0) {
          next = iter.next();
        }
        if (remainingOffset >= 0 || next == null) {
          endOfData();
        }
        remainingOffset = 0;
        numEntitiesReturned++;
        return next;
      }
    };
  }
}
