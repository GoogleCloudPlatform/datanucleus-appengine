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

import com.google.appengine.api.datastore.ShortBlob;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Serialized;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasBytesJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent
  private byte onePrimByte;

  @Persistent
  private Byte oneByte;

  @Persistent
  private byte[] primBytes;

  @Persistent
  private Byte[] bytes;

  @Persistent
  private List<Byte> byteList;

  @Persistent
  private Set<Byte> byteSet;

  @Persistent
  private Collection<Byte> byteCollection;

  @Persistent
  private ShortBlob shortBlob;

  @Persistent
  @Serialized
  private byte[] serializedPrimBytes;

  @Persistent
  @Serialized
  private Byte[] serializedBytes;

  @Persistent
  @Serialized
  private List<Byte> serializedByteList;

  @Persistent
  @Serialized
  private Set<Byte> serializedByteSet;

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

  public byte[] getSerializedPrimBytes() {
    return serializedPrimBytes;
  }

  public Byte[] getSerializedBytes() {
    return serializedBytes;
  }

  public List<Byte> getSerializedByteList() {
    return serializedByteList;
  }

  public Set<Byte> getSerializedByteSet() {
    return serializedByteSet;
  }

  public void setSerializedPrimBytes(byte[] serializedPrimBytes) {
    this.serializedPrimBytes = serializedPrimBytes;
  }

  public void setSerializedBytes(Byte[] serializedBytes) {
    this.serializedBytes = serializedBytes;
  }

  public void setSerializedByteList(List<Byte> serializedByteList) {
    this.serializedByteList = serializedByteList;
  }

  public void setSerializedByteSet(Set<Byte> serializedByteSet) {
    this.serializedByteSet = serializedByteSet;
  }

  public Collection<Byte> getByteCollection() {
    return byteCollection;
  }

  public void setByteCollection(Collection<Byte> byteCollection) {
    this.byteCollection = byteCollection;
  }
}
