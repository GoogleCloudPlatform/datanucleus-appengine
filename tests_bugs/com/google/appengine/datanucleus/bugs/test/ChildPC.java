package com.google.appengine.datanucleus.bugs.test;

import java.io.Serializable;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class ChildPC implements Serializable, Comparable<ChildPC> {
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

  public ChildPC(long id, String val) {
    this.embeddedObjId = id;
    this.value = val;
  }

  public String getValue() {
    return value;
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(ChildPC other) {
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
    if (obj == null || !(obj instanceof ChildPC)) {
      return false;
    }
    if (obj == this) {
      return true;
    }

    ChildPC other = (ChildPC)obj;
    if ((other.value == null && value != null) || (other.value != null && value == null)) {
      return false;
    }
    if (other.value.equals(value)) {
      return true;
    }

    return false;
  }
}