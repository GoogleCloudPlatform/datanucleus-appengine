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
package com.google.appengine.datanucleus.query;

import com.google.appengine.api.datastore.Entity;

import com.google.appengine.datanucleus.Utils;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * An {@link AbstractList} implementation that streams merge join entity results.
 *
 * @author Max Ross <maxr@google.com>
 */
class StreamingMergeJoinResult extends AbstractList<Entity> {

  /** Delegate for lazy loading of results. */
  private final LazyResult<Entity> lazyResult;

  /**
   * Constructs a StreamingMergeJoinResult.
   * @param lazyEntities The result of the query.
   */
  public StreamingMergeJoinResult(Iterable<Entity> lazyEntities) {
    this.lazyResult = new LazyResult<Entity>(lazyEntities, Utils.<Entity>identity(), false);
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public Entity get(int index) {
    return lazyResult.get(index);
  }

  /**
   * @throws NoSuchElementException if there are no more elements
   * to resolve.
   */
  void resolveNext() {
    lazyResult.resolveNext();
  }

  @Override
  public Iterator<Entity> iterator() {
    return lazyResult.listIterator();
  }

  @Override
  public ListIterator<Entity> listIterator() {
    return lazyResult.listIterator();
  }

  @Override
  public int size() {
    return lazyResult.size();
  }
}