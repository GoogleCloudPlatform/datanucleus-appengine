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

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.store.query.QueryTimeoutException;

import java.util.ConcurrentModificationException;

/**
 * Utility class that knows how to translate exceptions thrown by the datastore
 * api into the corresponding {@link NucleusException}.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class DatastoreExceptionTranslator {

  private DatastoreExceptionTranslator() {
  }

  public static NucleusException wrapIllegalArgumentException(IllegalArgumentException e) {
    // Bad input, so mark fatal to let user know not to retry.
    return new NucleusFatalUserException("Illegal argument", e);
  }

  public static NucleusDataStoreException wrapDatastoreFailureException(
      DatastoreFailureException e) {
    // could be a transient error so don't mark fatal
    return new NucleusDataStoreException("Datastore Failure", e);
  }

  static NucleusDataStoreException wrapConcurrentModificationException(
      ConcurrentModificationException e) {
    // do not mark fatal
    return new NucleusDataStoreException("Concurrent Modification", e);
  }

  static NucleusObjectNotFoundException wrapEntityNotFoundException(
      EntityNotFoundException e, Key key) {
    return new NucleusObjectNotFoundException(
        "Could not retrieve entity of kind " + key.getKind() + " with key " + key);
  }

  public static QueryTimeoutException wrapDatastoreTimeoutExceptionForQuery(final DatastoreTimeoutException e) {
    return new QueryTimeoutException(e.getMessage(), e);
  }
}
