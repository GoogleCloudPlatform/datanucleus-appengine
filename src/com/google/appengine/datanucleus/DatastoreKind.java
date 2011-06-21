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

import org.datanucleus.store.mapped.DatastoreIdentifier;

/**
 * Describes a 'kind' in the datastore.  This is datanucleus'
 * name for it, not the actual kind of the entities we store.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreKind implements DatastoreIdentifier {

  private final String identifier;

  public DatastoreKind(String identifier) {
    this.identifier = identifier;
  }

  public String getIdentifierName() {
    return identifier;
  }

  public String getFullyQualifiedName(boolean adapterCase) {
    // TODO(maxr): Figure out what to return here.
    return getIdentifierName();
  }

  public void setCatalogName(String s) {
  }

  public void setSchemaName(String s) {
  }

  public String getCatalogName() {
    return null;
  }

  public String getSchemaName() {
    return null;
  }
}
