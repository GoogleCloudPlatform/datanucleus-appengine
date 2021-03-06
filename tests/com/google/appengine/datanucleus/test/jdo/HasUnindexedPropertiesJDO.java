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
package com.google.appengine.datanucleus.test.jdo;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Text;

import java.util.List;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasUnindexedPropertiesJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent
  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  private String unindexedString;

  @Persistent
  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  private List<String> unindexedList;

  @Persistent
  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  private Blob unindexedBlob;

  @Persistent
  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  private Text unindexedText;

  @Persistent
  private Blob blob;

  @Persistent
  private Text text;

  public Long getId() {
    return id;
  }

  public String getUnindexedString() {
    return unindexedString;
  }

  public void setUnindexedString(String unindexedString) {
    this.unindexedString = unindexedString;
  }

  public List<String> getUnindexedList() {
    return unindexedList;
  }

  public void setUnindexedList(List<String> unindexedList) {
    this.unindexedList = unindexedList;
  }

  public Blob getUnindexedBlob() {
    return unindexedBlob;
  }

  public void setUnindexedBlob(Blob unindexedBlob) {
    this.unindexedBlob = unindexedBlob;
  }

  public Text getUnindexedText() {
    return unindexedText;
  }

  public void setUnindexedText(Text unindexedText) {
    this.unindexedText = unindexedText;
  }

  public Blob getBlob() {
    return blob;
  }

  public void setBlob(Blob blob) {
    this.blob = blob;
  }

  public Text getText() {
    return text;
  }

  public void setText(Text text) {
    this.text = text;
  }
}