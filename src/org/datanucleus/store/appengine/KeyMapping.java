// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.store.mapped.mapping.SerialisedMapping;

/**
 * Custom mapping class for {@link Key}.  This lets us have
 * relations involving pojos where the pk is a {@link Key}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class KeyMapping extends SerialisedMapping {

  @Override
  public Class getJavaType() {
    return Key.class;
  }
}
