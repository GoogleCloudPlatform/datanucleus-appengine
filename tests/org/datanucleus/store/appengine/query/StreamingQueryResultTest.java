// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import junit.framework.TestCase;
import com.google.apphosting.api.datastore.Entity;
import com.google.common.collect.Lists;
import com.google.common.base.Function;

import java.util.Iterator;
import java.util.Collections;
import java.util.ListIterator;

/**
 * @author Max Ross <maxr@google.com>
 */
public class StreamingQueryResultTest extends TestCase {

  public void testEquality() {
    StreamingQueryResult sqr1 = new StreamingQueryResult(null, Collections.<Entity>emptyList(), null);
    StreamingQueryResult sqr2 = new StreamingQueryResult(null, Collections.<Entity>emptyList(), null);
    assertTrue(sqr1.equals(sqr1));
    assertFalse(sqr1.equals(sqr2));
  }

  private static final Function<Entity, Object> NULL_FUNC = new Function<Entity, Object>() {
    public Object apply(Entity entity) {
      return entity;
    }
  };

  private static class CountingIterable implements Iterable<Entity> {
    private final Iterable<Entity> iterable;
    private int nextCount = 0;

    private CountingIterable(Iterable<Entity> iterable) {
      this.iterable = iterable;
    }

    public Iterator<Entity> iterator() {
      return new CountingIterator(iterable.iterator());
    }

    private class CountingIterator implements Iterator<Entity> {
      private final Iterator<Entity> iter;

      private CountingIterator(Iterator<Entity> iter) {
        this.iter = iter;
      }

      public boolean hasNext() {
        return iter.hasNext();
      }

      public Entity next() {
        nextCount++;
        return iter.next();
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }

  public void testSize_FreshIterator() {
    CountingIterable iterable = new CountingIterable(Lists.<Entity>newArrayList());
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    assertEquals(0, sqr.size());
    assertEquals(0, iterable.nextCount);

    Entity e = null;
    iterable = new CountingIterable(Lists.newArrayList(e));
    sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    assertEquals(1, sqr.size());
    assertEquals(1, iterable.nextCount);

    iterable = new CountingIterable(Lists.newArrayList(e, e));
    sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    assertEquals(2, sqr.size());
    assertEquals(2, iterable.nextCount);
  }

  public void testSize_PartiallyConsumedIterator() {
    Entity e = null;
    CountingIterable iterable = new CountingIterable(Lists.newArrayList(e, e, e));
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    sqr.resolveNext();
    assertEquals(1, iterable.nextCount);
    assertEquals(3, sqr.size());
    assertEquals(3, iterable.nextCount);
  }

  public void testSize_ExhaustedIterator() {
    Entity e = null;
    CountingIterable iterable = new CountingIterable(Lists.newArrayList(e, e));
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    sqr.resolveNext();
    sqr.resolveNext();
    assertEquals(2, iterable.nextCount);
    assertEquals(2, sqr.size());
    assertEquals(2, iterable.nextCount);
  }

  public void testGet_FreshIterator() {
    CountingIterable iterable = new CountingIterable(Lists.<Entity>newArrayList());
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    try {
      sqr.get(0);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(0, iterable.nextCount);

    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    iterable = new CountingIterable(Lists.newArrayList(e1, e2));
    sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    assertEquals(e1, sqr.get(0));
    assertEquals(1, iterable.nextCount);
    assertEquals(e2, sqr.get(1));
    assertEquals(2, iterable.nextCount);

    try {
      sqr.get(3);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(2, iterable.nextCount);
  }

  public void testGet_PartiallyConsumedIterator() {
    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    CountingIterable iterable = new CountingIterable(Lists.<Entity>newArrayList(e1, e2));
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    sqr.resolveNext();
    assertEquals(1, iterable.nextCount);
    assertEquals(e1, sqr.get(0));
    assertEquals(1, iterable.nextCount);
    assertEquals(e2, sqr.get(1));
    assertEquals(2, iterable.nextCount);

    try {
      sqr.get(3);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(2, iterable.nextCount);
  }

  public void testGet_ExhaustedIterator() {
    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    CountingIterable iterable = new CountingIterable(Lists.<Entity>newArrayList(e1, e2));
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    sqr.resolveNext();
    sqr.resolveNext();
    assertEquals(2, iterable.nextCount);
    assertEquals(e1, sqr.get(0));
    assertEquals(2, iterable.nextCount);
    assertEquals(e2, sqr.get(1));
    assertEquals(2, iterable.nextCount);

    try {
      sqr.get(3);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(2, iterable.nextCount);
  }

  // This implicitly tests the iterator() method as well since iterator() just
  // delegates to listIterator()
  public void testListIterator() {
    CountingIterable iterable = new CountingIterable(Lists.<Entity>newArrayList());
    StreamingQueryResult sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    assertFalse(sqr.listIterator().hasNext());

    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    Entity e3 = new Entity("yar");
    Entity e4 = new Entity("yar");
    iterable = new CountingIterable(Lists.<Entity>newArrayList(e1, e2, e3, e4));
    sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);

    ListIterator listIter = sqr.listIterator();
    assertTrue(listIter.hasNext());
    assertEquals(e1, listIter.next());
    assertEquals(1, iterable.nextCount);
    assertEquals(e1, sqr.get(0));
    assertEquals(1, iterable.nextCount);

    assertEquals(e2, listIter.next());
    assertEquals(2, iterable.nextCount);
    // Calls to the iterator make more data available to get() so nextCount
    // does not increment.
    assertEquals(e1, sqr.get(1));
    assertEquals(2, iterable.nextCount);

    // now we work our way backwards
    assertEquals(2, listIter.nextIndex());
    assertEquals(1, listIter.previousIndex());
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    assertEquals(e2, listIter.previous());
    assertTrue(listIter.hasNext());
    assertEquals(3, iterable.nextCount);
    assertTrue(listIter.hasPrevious());

    assertEquals(1, listIter.nextIndex());
    assertEquals(0, listIter.previousIndex());
    assertEquals(e1, listIter.previous());
    assertEquals(3, iterable.nextCount);
    assertFalse(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    assertEquals(-1, listIter.previousIndex());

    // now we go forwards again
    assertEquals(e1, listIter.next());
    assertEquals(1, listIter.nextIndex());
    assertEquals(0, listIter.previousIndex());
    assertEquals(3, iterable.nextCount);
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());

    assertEquals(e2, listIter.next());
    assertEquals(2, listIter.nextIndex());
    assertEquals(1, listIter.previousIndex());
    assertEquals(3, iterable.nextCount);
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    assertEquals(3, iterable.nextCount);

    assertEquals(e3, listIter.next());
    assertEquals(3, iterable.nextCount);
    assertEquals(3, listIter.nextIndex());
    assertEquals(2, listIter.previousIndex());
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    // the call to hasNext() results in a fetch
    assertEquals(4, iterable.nextCount);

    assertEquals(e4, listIter.next());
    assertEquals(4, listIter.nextIndex());
    assertEquals(3, listIter.previousIndex());
    assertEquals(4, iterable.nextCount);
    assertTrue(listIter.hasPrevious());
    assertFalse(listIter.hasNext());

    iterable = new CountingIterable(Lists.<Entity>newArrayList(e1, e2));
    sqr = new StreamingQueryResult(null, iterable, NULL_FUNC);
    listIter = sqr.listIterator();
    assertTrue(listIter.hasNext());
    assertEquals(e1, listIter.next());
    assertEquals(1, iterable.nextCount);

    // Call to get makes more data available to the iterator.
    sqr.get(1);
    assertEquals(2, iterable.nextCount);
    assertTrue(listIter.hasNext());
    assertEquals(e2, listIter.next());
    assertEquals(2, iterable.nextCount);
  }

}
