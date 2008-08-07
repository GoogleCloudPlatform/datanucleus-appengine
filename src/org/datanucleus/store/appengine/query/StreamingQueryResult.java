// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.Query;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.ArrayList;

import com.google.apphosting.api.datastore.Entity;
import com.google.common.collect.Lists;
import com.google.common.collect.AbstractIterator;
import com.google.common.base.Function;

/**
 * An {@link AbstractQueryResult} implementation that streams results, converting
 * from {@link Entity Entities} to POJOs as clients access the data.
 *
 * @author Max Ross <maxr@google.com>
 */
class StreamingQueryResult extends AbstractQueryResult {

  /**
   * A function that translates an {@link Entity} to a pojo.
   */
  private final Function<Entity, Object> entityToPojoFunc;

  /**
   * Returns {@link Entity Entities} from the datastore.
   */
  private final Iterator<Entity> lazyEntityIterator;

  /**
   * Pojos that have been created by transforming elements returned by
   * {@link #lazyEntityIterator}.  We append to this list each time
   * a new element is read from {@link #lazyEntityIterator}.
   *
   * This member is an {@link ArrayList} rather than a {@link List} to make
   * it clear that we depend on the constant time indexing that
   * {@link ArrayList} provides.
   */
  private final ArrayList<Object> resolvedPojos = Lists.newArrayList();

  /**
   * Constrctus a StreamingQueryResult
   *
   * @param query The query which yields the results.
   * @param lazyEntities The result of the query.
   * @param entityToPojoFunc A function that can convert a {@link Entity}
   * into a pojo.
   */
  public StreamingQueryResult(Query query, Iterable<Entity> lazyEntities,
      Function<Entity, Object> entityToPojoFunc) {
    super(query);
    this.lazyEntityIterator = lazyEntities.iterator();
    this.entityToPojoFunc = entityToPojoFunc;
  }

  protected void closingConnection() {
    // If you close the connection before all results are returned, that's fine.
  }

  protected void closeResults() {
    // Do we need to actually close anything?
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  public Object get(int index) {
    // See if we've resolved the pojo at this index.  If we have we just return
    // the pojo at that index.  If we haven't and our iterator still has more
    // elements we need to keep resolving until we actually have an element
    // at the requested index.
    if (index >= resolvedPojos.size() && lazyEntityIterator.hasNext()) {
      // Stop resolving if the iterator doesn't have any more data.
      // This means we may stop before we get to the requested index, but
      // that's ok.
      for (int i = resolvedPojos.size(); i <= index && lazyEntityIterator.hasNext(); i++) {
        resolveNext();
      }
    }
    // If the index is out of range we'll get an exception, and that's
    // fine.  This is consistent with the List interface.
    return resolvedPojos.get(index);
  }

  /**
   * @throws NoSuchElementException if there are no more elements
   * to resolve.
   */
  void resolveNext() {
    resolvedPojos.add(entityToPojoFunc.apply(lazyEntityIterator.next()));
  }

  public Iterator iterator() {
    return listIterator();
  }

  public ListIterator listIterator() {
    if (!lazyEntityIterator.hasNext()) {
      return resolvedPojos.listIterator();
    }
    return new AbstractListIterator() {
      // The index member in our parent means something slightly different
      // so we maintain our own.
      private int curIndex = 0;
      protected Object computeNext() {
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
    };
  }

  public int size() {
    // We're forced to resolve everything.
    while (lazyEntityIterator.hasNext()) {
      resolveNext();
    }
    return resolvedPojos.size();
  }

  /**
   * {@link AbstractListIterator implementation that uses the Iterator
   * and the list of resolved pojos that belong to the enclosing member
   * to implement the {@link ListIterator} interface.
   */
  private abstract class AbstractListIterator
      extends AbstractIterator<Object> implements ListIterator<Object> {

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

    public Object previous() {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      Object result = resolvedPojos.get(previousIndex());
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

    public boolean hasNext() {
      // If the current index isn't as far forward as we've ever been
      // we know we have more data ahead of us.  If the current index
      // is as far forward as we've ever been, delegate to our parent
      // to see if we have more data.
      return curIndex < maxIndex || super.hasNext();
    }

    public Object next() {
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
      Object result = super.next();
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
}
