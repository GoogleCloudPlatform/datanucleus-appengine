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
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasStringAncestorKeyPkJPA {

  // I think according to the spec this isn't valid - you can't
  // have fields of custom types that aren't part of the PK.
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Key key;

  @Extension(vendorName="datanucleus", key="gae.parent-pk", value="true")
  private String ancestorKey;

  public Key getKey() {
    return key;
  }

  public void setKey(Key key) {
    this.key = key;
  }

  public String getAncestorKey() {
    return ancestorKey;
  }

  public void setAncestorKey(String ancestorKey) {
    this.ancestorKey = ancestorKey;
  }
}
