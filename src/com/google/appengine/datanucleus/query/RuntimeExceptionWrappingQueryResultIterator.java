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

import java.util.List;

import org.datanucleus.api.ApiAdapter;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Index;
import com.google.appengine.api.datastore.QueryResultIterator;

/**
 * @author Max Ross <maxr@google.com>
 */
class RuntimeExceptionWrappingQueryResultIterator extends RuntimeExceptionWrappingIterator
    implements QueryResultIterator<Entity> {

  RuntimeExceptionWrappingQueryResultIterator(ApiAdapter api, RuntimeExceptionWrappingIterable iterable,
                                              QueryResultIterator<Entity> inner) {
    super(api, inner, iterable);
  }

  public Cursor getCursor() {
    return ((QueryResultIterator<Entity>) inner).getCursor();
  }

  public List<Index> getIndexList() {
    throw new UnsupportedOperationException("This method is not supported");
  }
}