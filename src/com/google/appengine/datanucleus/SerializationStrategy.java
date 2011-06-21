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

import com.google.appengine.api.datastore.Blob;

/**
 * A strategy for serializing objects to the datastore.
 *
 * This class is part of the public interface of the DataNucleus App Engine
 * Plugin and can be safely referenced in user code.
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
