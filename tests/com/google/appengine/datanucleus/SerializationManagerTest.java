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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.datanucleus.test.HasSerializableJDO;

import junit.framework.TestCase;

import org.datanucleus.JDOClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ExtensionMetaData;

import java.io.Serializable;

/**
 * SerializationManager tests.
 *
 * @author Max Ross <maxr@google.com>
 */
public class SerializationManagerTest extends TestCase {

  public void testDeserialize_NonObjectBytes() {
    try {
      SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.deserialize(new Blob("yar".getBytes()), getClass());
      fail("Expcted NucleusException");
    } catch (NucleusException ne) {
      // good
    }
  }

  public static final class MySerializable1 implements Serializable {}
  public static final class MySerializable2 implements Serializable {}

  public void testDeserialize_BytesAreOfWrongType() {
    MySerializable1 ser1 = new MySerializable1();
    Blob ser1Blob = SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.serialize(ser1);
    try {
      SerializationManager.DEFAULT_SERIALIZATION_STRATEGY.deserialize(ser1Blob, MySerializable2.class);
      fail("Expcted NucleusException");
    } catch (NucleusException ne) {
      // good
    }
  }

  public void testGetDefaultSerializer() {
    SerializationManager mgr = new SerializationManager();
    AbstractMemberMetaData ammd = new AbstractMemberMetaData(null, "yar") {};
    assertSame(SerializationManager.DEFAULT_SERIALIZATION_STRATEGY, mgr.getSerializationStrategy(new JDOClassLoaderResolver(), ammd));
  }

  public void testGetCustomSerializer() {
    SerializationManager mgr = new SerializationManager();
    AbstractMemberMetaData ammd = new AbstractMemberMetaData(null, "yar") {
      @Override
      public ExtensionMetaData[] getExtensions() {
        ExtensionMetaData emd = new ExtensionMetaData(
            "datanucleus",
            SerializationManager.SERIALIZATION_STRATEGY_KEY,
            HasSerializableJDO.ProtocolBufferSerializationStrategy.class.getName());
        return new ExtensionMetaData[] {emd};
      }
    };
    SerializationStrategy serializationStrategy = mgr.getSerializationStrategy(new JDOClassLoaderResolver(), ammd);
    assertTrue(serializationStrategy instanceof HasSerializableJDO.ProtocolBufferSerializationStrategy);
  }

  public void testGetCustomSerializer_BadClass() {
    SerializationManager mgr = new SerializationManager();
    AbstractMemberMetaData ammd = new AbstractMemberMetaData(null, "yar") {
      @Override
      public ExtensionMetaData[] getExtensions() {
        ExtensionMetaData emd = new ExtensionMetaData(
            "datanucleus",
            SerializationManager.SERIALIZATION_STRATEGY_KEY,
            getClass().getName());
        return new ExtensionMetaData[] {emd};
      }
    };
    try {
      mgr.getSerializationStrategy(new JDOClassLoaderResolver(), ammd);
      fail("Expectd NucleusException");
    } catch (NucleusException ne) {
      // good
    }
  }
}