// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Version;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasVersionJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  private String value;

  @Version
  private long version;

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getId() {
    return id;
  }

  public long getVersion() {
    return version;
  }
}
