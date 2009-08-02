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

import com.google.appengine.api.datastore.ShortBlob;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasBytesJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Long id;

  private byte onePrimByte;
  private Byte oneByte;

  private byte[] primBytes;

  private Byte[] bytes;

  @Basic
  private List<Byte> byteList;

  @Basic
  private Set<Byte> byteSet;

  @Basic
  private Collection<Byte> byteCollection;

  @Lob
  private byte[] serializedPrimBytes;

  @Lob
  private Byte[] serializedBytes;

  @Lob
  private List<Byte> serializedByteList;

  @Lob
  private Set<Byte> serializedByteSet;

  @Basic
  private ShortBlob shortBlob;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public byte[] getPrimBytes() {
    return primBytes;
  }

  public void setPrimBytes(byte[] primBytes) {
    this.primBytes = primBytes;
  }

  public Byte[] getBytes() {
    return bytes;
  }

  public void setBytes(Byte[] bytes) {
    this.bytes = bytes;
  }

  public byte getOnePrimByte() {
    return onePrimByte;
  }

  public void setOnePrimByte(byte onePrimByte) {
    this.onePrimByte = onePrimByte;
  }

  public Byte getOneByte() {
    return oneByte;
  }

  public void setOneByte(Byte oneByte) {
    this.oneByte = oneByte;
  }

  public ShortBlob getShortBlob() {
    return shortBlob;
  }

  public void setShortBlob(ShortBlob shortBlob) {
    this.shortBlob = shortBlob;
  }

  public List<Byte> getByteList() {
    return byteList;
  }

  public void setByteList(List<Byte> byteList) {
    this.byteList = byteList;
  }

  public Set<Byte> getByteSet() {
    return byteSet;
  }

  public void setByteSet(Set<Byte> byteSet) {
    this.byteSet = byteSet;
  }

  public Collection<Byte> getByteCollection() {
    return byteCollection;
  }

  public void setByteCollection(Collection<Byte> byteCollection) {
    this.byteCollection = byteCollection;
  }

  public byte[] getSerializedPrimBytes() {
    return serializedPrimBytes;
  }

  public void setSerializedPrimBytes(byte[] serializedPrimBytes) {
    this.serializedPrimBytes = serializedPrimBytes;
  }

  public Byte[] getSerializedBytes() {
    return serializedBytes;
  }

  public void setSerializedBytes(Byte[] serializedBytes) {
    this.serializedBytes = serializedBytes;
  }

  public List<Byte> getSerializedByteList() {
    return serializedByteList;
  }

  public void setSerializedByteList(List<Byte> serializedByteList) {
    this.serializedByteList = serializedByteList;
  }

  public Set<Byte> getSerializedByteSet() {
    return serializedByteSet;
  }

  public void setSerializedByteSet(Set<Byte> serializedByteSet) {
    this.serializedByteSet = serializedByteSet;
  }
}
