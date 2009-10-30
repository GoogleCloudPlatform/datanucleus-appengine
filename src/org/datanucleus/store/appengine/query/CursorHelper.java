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

import java.util.List;

/**
 * Utilities for working with {@link Cursor} through the JDO and JPA
 * Query apis.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
class CursorHelper {

  static final String QUERY_CURSOR_PROPERTY_NAME = "gae.query.cursor";

  CursorHelper() {}

  public static Cursor getCursor(List<?> list) {
    if (list instanceof StreamingQueryResult) {
      StreamingQueryResult sqr = (StreamingQueryResult) list;
      return sqr.getCursor();
    }
    return null;
  }
}