/**********************************************************************
Copyright (c) 2011 Google Inc.

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
package com.google.appengine.datanucleus.test;

import java.io.Serializable;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class EmbeddedChildPC implements Serializable, Comparable<EmbeddedChildPC> {
  @SuppressWarnings("unused")
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")    
  private String embeddedObjKey;

  @SuppressWarnings("unused")
  @Persistent
  @Extension(vendorName="datanucleus", key="gae.pk-id", value="true")
  private Long embeddedObjId;

  @Persistent
  private String value;

  public EmbeddedChildPC(long id, String val) {
    this.embeddedObjId = id;
    this.value = val;
  }

  public String getValue() {
    return value;
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(EmbeddedChildPC other) {
    if (other == null) {
      return -1;
    }
    if ((other.value == null && value != null) || (other.value != null && value == null)) {
      return -1;
    }
    if (other.value.equals(value)) {
      return 0;
    }
    return 1;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof EmbeddedChildPC)) {
      return false;
    }
    if (obj == this) {
      return true;
    }

    EmbeddedChildPC other = (EmbeddedChildPC)obj;
    if ((other.value == null && value != null) || (other.value != null && value == null)) {
      return false;
    }
    if (other.value.equals(value)) {
      return true;
    }

    return false;
  }
}