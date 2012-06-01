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
package com.google.appengine.datanucleus.test.jpa;

import org.datanucleus.api.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasStringAncestorStringPkJPA {

  @Extension(vendorName="datanucleus", key="gae.parent-pk", value="true")
  private String ancestorId;

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  public HasStringAncestorStringPkJPA() {
  }

  public HasStringAncestorStringPkJPA(String ancestorId) {
    this(ancestorId, null);
  }

  public HasStringAncestorStringPkJPA(String ancestorId, String id) {
    this.ancestorId = ancestorId;
    this.id = id;
  }

  public String getAncestorId() {
    return ancestorId;
  }

  public String getId() {
    return id;
  }
}
