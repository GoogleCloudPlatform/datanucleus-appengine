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
package com.google.appengine.datanucleus.test;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Text;

import org.datanucleus.jpa.annotations.Extension;

import java.util.List;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasUnindexedPropertiesJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Long id;

  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  private String unindexedString;

  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  private List<String> unindexedList;

  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  @Basic
  private Blob unindexedBlob;

  @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true")
  @Basic
  private Text unindexedText;

  @Basic
  private Blob blob;

  @Basic
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