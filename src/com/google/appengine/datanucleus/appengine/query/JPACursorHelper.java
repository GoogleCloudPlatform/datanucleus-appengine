/*
 * Copyright (C) 2010 Google Inc
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
package com.google.appengine.datanucleus.appengine.query;

import com.google.appengine.api.datastore.Cursor;

import javax.persistence.Query;

/**
 * Utilities for working with {@link Cursor} through the JPA
 * {@link Query} api.
 * <br>
 * To add a Cursor to a query, set the Cursor as an extension:
 * <blockquote>
 * <pre>
 * Query q = em.createQuery(Flight.class);
 * q.setFirstResult(100);
 * q.setMaxResults(100);
 * q.setHint(JpaCursorHelper.CURSOR_HINT, cursor);
 * List<Flight> flights = q.getResultList();
 * </pre>
 * </blockquote>
 * Note that in this esample {@code cursor} can be either of type
 * {@link Cursor} or a {@link String} representation of Cursor obtained by
 * calling {@link Cursor#toWebSafeString()}.
 *<br>
 * To extract a Cursor from a result set, call {@link #getCursor(java.util.List)}
 * with the result set as the argument.
 *<br>
 * This class is part of the public api of the DataNucleus App Engine plugin
 * and can be safely used.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public final class JPACursorHelper extends CursorHelper {

  public static final String CURSOR_HINT = QUERY_CURSOR_PROPERTY_NAME;

  private JPACursorHelper() {}
}