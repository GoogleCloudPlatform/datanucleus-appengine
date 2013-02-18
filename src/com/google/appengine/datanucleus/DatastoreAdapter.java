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

import org.datanucleus.store.mapped.IdentifierType;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.mapping.MappingManager;

import com.google.appengine.datanucleus.mapping.DatastoreMappingManager;

import java.util.Collection;
import java.util.HashSet;

/**
 * Adapter for the App Engine datastore.
 * This interface is designed around RDBMS so the majority doesn't apply here
 * and besides which the GAE/J plugin really ought not extend MappedStoreManager
 * for that very reason.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreAdapter implements org.datanucleus.store.mapped.DatastoreAdapter {

  public DatastoreAdapter() {
    supportedOptions.add(IDENTITY_COLUMNS);
    supportedOptions.add(IDENTIFIERS_MIXEDCASE);
    supportedOptions.add(IDENTIFIERS_LOWERCASE);
    supportedOptions.add(IDENTIFIERS_UPPERCASE);
  }

  private final Collection<String> supportedOptions = new HashSet<String>();

  public MappingManager getMappingManager(MappedStoreManager mappedStoreManager) {
    return new DatastoreMappingManager(mappedStoreManager);
  }

  public Collection<String> getSupportedOptions() {
    return supportedOptions;
  }

  public boolean supportsOption(String option) {
    return supportedOptions.contains(option);
  }

  public String getIdentifierQuoteString() {
    return "\"";
  }

  public int getDatastoreIdentifierMaxLength(IdentifierType identifierType) {
    return 99;
  }

  public int getMaxForeignKeys() {
    return 999;
  }

  public int getMaxIndexes() {
    return 999;
  }

  public String getCatalogSeparator() {
    return null;
  }

  public String toString() {
    return "Google App Engine Datastore";
  }
}
