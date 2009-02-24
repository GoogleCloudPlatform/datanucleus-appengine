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

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolMessage;
import com.google.apphosting.api.DatastorePb;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.appengine.SerializationStrategy;

import java.io.Serializable;

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
public class HasSerializableJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent(serialized = "true")
  private Yam yam;

  @Persistent(serialized = "true")
  @Extension(vendorName = "datanucleus", key="serialization-strategy",
             value="org.datanucleus.test.HasSerializableJDO$ProtocolBufferSerializationStrategy")
  private DatastorePb.Query query;

  public Yam getYam() {
    return yam;
  }

  public void setYam(Yam yam) {
    this.yam = yam;
  }

  public DatastorePb.Query getQuery() {
    return query;
  }

  public void setQuery(DatastorePb.Query query) {
    this.query = query;
  }

  public Long getId() {
    return id;
  }

  public static class ProtocolBufferSerializationStrategy implements SerializationStrategy {

    public Blob serialize(Object obj) {
      ProtocolMessage<?> pb = (ProtocolMessage<?>) obj;
      return new Blob(pb.toByteArray());
    }

    public Object deserialize(Blob blob, Class<?> targetClass) {
      if (!ProtocolMessage.class.isAssignableFrom(targetClass)) {
        throw new NucleusException("Not a pb class!");
      }
      ProtocolMessage<?> pb;
      try {
        pb = (ProtocolMessage<?>) targetClass.newInstance();
      } catch (Exception e) {
        throw new NucleusException("Exception trying to instantiate", e);
      }
      pb.parseFrom(blob.getBytes());
      return pb;
    }
  }
  public static class Yam implements Serializable {
    private String str1;
    private String str2;

    public String getStr1() {
      return str1;
    }

    public void setStr1(String str1) {
      this.str1 = str1;
    }

    public String getStr2() {
      return str2;
    }

    public void setStr2(String str2) {
      this.str2 = str2;
    }
  }
}
