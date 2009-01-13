// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Blob;

/**
 * A strategy for serializing objects to the datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public interface SerializationStrategy {

  /**
   * Transforms the given object into a {@link Blob}.
   * @param obj The object to be transformed.
   * @return The {@link Blob} representation of the given object.
   */
  Blob serialize(Object obj);

  /**
   * Transforms the given blob into an object of the given class.
   * @param blob The blob to be transformed.
   * @param targetClass The class of the object into which the blob should be
   * transformed.
   * @return The <code>targetClass</code> representation of the given blob.
   */
  Object deserialize(Blob blob, Class<?> targetClass);
}
