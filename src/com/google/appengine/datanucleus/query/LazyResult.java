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
import com.google.appengine.datanucleus.Utils.Function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * Provides both {@link Iterator Iterators} and
 * {@link ListIterator ListIterators} on top of a data source that reads
 * pending data as lazily as possible.
 *
 * @author Max Ross <maxr@google.com>
 */
class LazyResult<T> implements Iterable<T> {

  /**
   * A function that translates an {@link Entity} to an instance of type {@code T}.
   */
  private final Function<Entity, T> entityTransformer;

  /**
   * Returns {@link Entity Entities} from the datastore.
   */
  private final Iterator<Entity> lazyEntityIterator;

  /**
   * Pojos that have been created by transforming elements returned by
   * {@link #lazyEntityIterator}.  We append to this list each time
   * a new element is read from {@link #lazyEntityIterator}.
   *
   * This member is an {@link java.util.ArrayList} rather than a {@link java.util.List} to make
   * it clear that we depend on the constant time indexing that
   * {@link java.util.ArrayList} provides.
   */
  private final ArrayList<T> resolvedPojos = Utils.newArrayList();

  /** List of the Keys of the entities in this result (used when caching the results). */
  private final List<Object> resultKeys;

  /**
   * Constructor for a lazy result.
   * @param lazyEntities The result of the query.
   * @param entityTransformer A function that can convert a {@link Entity} into a pojo.
   * @param cacheKeys Whether we should cache the Keys of the entities, to be used later caching the query results
   */
  public LazyResult(Iterable<Entity> lazyEntities,
      Function<Entity, T> entityTransformer, boolean cacheKeys) {
    this.lazyEntityIterator = lazyEntities.iterator();
    this.entityTransformer = entityTransformer;
    this.resultKeys = (cacheKeys ? new ArrayList() : null);
  }

  T get(int index) {
    // See if we've resolved the pojo at this index.  If we have we just return
    // the pojo at that index.  If we haven't and our iterator still has more
    // elements we need to keep resolving until we actually have an element
    // at the requested index.
    if (index >= resolvedPojos.size() && lazyEntityIterator.hasNext()) {
      // Stop resolving if the iterator doesn't have any more data.
      // This means we may stop before we get to the requested index, but that's ok.
      for (int i = resolvedPojos.size(); i <= index && lazyEntityIterator.hasNext(); i++) {
        resolveNext();
      }
    }
    // If the index is out of range we'll get an exception, and that's fine. Consistent with the List interface.
    return resolvedPojos.get(index);
  }

  /**
   * @throws java.util.NoSuchElementException if there are no more elements to resolve.
   */
  void resolveNext() {
    Entity entity = lazyEntityIterator.next();
    resolvedPojos.add(entityTransformer.apply(entity));
    if (resultKeys != null) {
      resultKeys.add(entity.getKey());
    }
  }

  public Iterator<T> iterator() {
    return listIterator();
  }

  ListIterator<T> listIterator() {
    if (!lazyEntityIterator.hasNext()) {
      return resolvedPojos.listIterator();
    }
    return new LazyAbstractListIterator();
  }

  public int size() {
    // We're forced to resolve everything.
    resolveAll();
    return resolvedPojos.size();
  }

  void resolveAll() {
    while (lazyEntityIterator.hasNext()) {
      resolveNext();
    }
  }

  /**
   * Accessor for the result keys.
   * Will be null if you set "cacheKeys" as false on construction.
   * @return Entity keys
   */
  public List getEntityKeys() {
    return resultKeys;
  }

  /**
   * {@link AbstractListIterator implementation that uses the Iterator
   * and the list of resolved pojos that belong to the enclosing member
   * to implement the {@link ListIterator} interface.
   */
  private abstract class AbstractListIterator
      extends AbstractIterator<T> implements ListIterator<T> {

    /**
     * The index of the element that will be returned when {@link #next()} is
     * called.
     */
    private int curIndex = 0;

    /**
     * The largest value that curIndex has ever had.
     */
    private int maxIndex = 0;

    public boolean hasPrevious() {
      return previousIndex() != -1;
    }

    public T previous() {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      T result = resolvedPojos.get(previousIndex());
      curIndex--;
      return result;
    }

    public int nextIndex() {
      // According to the spec, this has to return the size of the Collection
      // if there are no more results.  Since we increment
      // on each call to next(), this will be the case.
      return curIndex;
    }

    public int previousIndex() {
      return curIndex - 1;
    }

    @Override
    public boolean hasNext() {
      // If the current index isn't as far forward as we've ever been
      // we know we have more data ahead of us.  If the current index
      // is as far forward as we've ever been, delegate to our parent
      // to see if we have more data.
      return curIndex < maxIndex || super.hasNext();
    }

    @Override
    public T next() {
      if (curIndex < maxIndex) {
        // It the current index isn't as far forward as we've ever been
        // we know the data at the current index has already been resolved,
        // so just return it directly from the resolved collection.
        return resolvedPojos.get(curIndex++);
      }
      // Current index is as far forward as we've ever been so delegate to our
      // parent to get the next element.

      // Don't update any state until the next() call is successful, that way
      // the iterator is still in a consistent state if someone iterates off the end.
      T result = super.next();
      // curIndex must be equal to maxIndex or else we would have already
      // returned, so increment curIndex and set maxIndex to the new value.
      maxIndex = ++curIndex;
      return result;
    }

    public void set(Object o) {
      throw new UnsupportedOperationException();
    }

    public void add(Object o) {
      throw new UnsupportedOperationException();
    }
  }

  final class LazyAbstractListIterator extends AbstractListIterator {
    // The index member in our parent means something slightly different
    // so we maintain our own.
    private int curIndex = 0;

    @Override
    protected T computeNext() {
      if (curIndex >= resolvedPojos.size()) {
        // We have not yet resolved an Entity at the current index
        if (!lazyEntityIterator.hasNext()) {
          // There are no more entities to resolve, so we're done.
          endOfData();
          return null;
        }
        resolveNext();
      }
      return resolvedPojos.get(curIndex++);
    }

    Iterator<?> getInnerIterator() {
      return lazyEntityIterator;
    }
  }
}