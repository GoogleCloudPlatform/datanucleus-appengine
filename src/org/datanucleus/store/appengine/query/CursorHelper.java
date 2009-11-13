/*
 * Copyright (C) 2009 Max Ross.
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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.QueryResultIterator;

import java.util.Iterator;
import java.util.List;

/**
 * Utilities for extracting {@link Cursor Cursors} from query results.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
class CursorHelper {

  static final String QUERY_CURSOR_PROPERTY_NAME = "gae.query.cursor";

  CursorHelper() {}

  /**
   * Extract a {@link Cursor} from the provided {@link List}.  The Cursor
   * points to the last element in the list.  A query that
   * is executed using the returned Cursor will start scanning directly after
   * the last element in the list.
   * <b>
   * A Cursor will only be available if the List is a query result and the
   * query had a limit set.
   *
   * @param list The {@link List} from which to extract a {@link Cursor}.
   * @return The {@link Cursor}, or {@code null} if no Cursor is available for
   * the provided list.
   */
  public static Cursor getCursor(List<?> list) {
    if (list instanceof StreamingQueryResult) {
      StreamingQueryResult sqr = (StreamingQueryResult) list;
      return sqr.getEndCursor();
    }
    return null;
  }

  /**
   * Extract a {@link Cursor} from the provided {@link Iterator}.  The Cursor
   * points to the element most recently returned by the iterator.  A query
   * that is executed using the returned Cursor will start scanning directly
   * after this element.
   * <b>
   * A Cursor will only be available if the Iterator was created from a query
   * result and the query did not have a limit set.
   *
   * @param iter The {@link Iterator} from which to extract a {@link Cursor}.
   * @return The {@link Cursor}, or {@code null} if no Cursor is available for
   * the provided cursor.
   */
  public static Cursor getCursor(Iterator<?> iter) {
    if (iter instanceof LazyResult.LazyAbstractListIterator) {
      Iterator<?> innerIter = ((LazyResult.LazyAbstractListIterator) iter).getInnerIterator();
      if (innerIter instanceof QueryResultIterator) {
        return ((QueryResultIterator) innerIter).getCursor();
      }
    }
    return null;
  }
}