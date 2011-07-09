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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ExtensionMetaData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Helper used by {@link DatastoreFieldManager} to process serialized fields.
 *
 * Datanucleus' standard behavior is to use java serialization, so that is our
 * standard behavior as well.  However, App Engine ORM supports custom
 * serialization strategies as an extension to this feature.  Developers can
 * create a class that implements the {@link SerializationStrategy} interface
 * and then associate that class with the serialized member using
 * Datanucleus' extension facility in either xml or via annotation.  The
 * following is an example using annotations:
 *
 * <pre>
 * @Persistent(serialized = "true")
 * @Extension(vendorName = "datanucleus", key="serialization-strategy",
              value="com.google.appengine.MySerializationStrategy")
 * private MyClass myClass;
 * </pre>
 *
 * In the above example, the value assigned to the <code>myClass</code> member
 * will be transformed into a {@link Blob} by an instance of
 * <code>MySerializationStrategy> before being written to the datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public class SerializationManager {

  /**
   * The key for the serialization strategy override extension.
   */
  static final String SERIALIZATION_STRATEGY_KEY = "serialization-strategy";

  /**
   * Default serialization strategy - standard java serialization.
   */
  public static final SerializationStrategy DEFAULT_SERIALIZATION_STRATEGY = new SerializationStrategy() {

    /**
     * Serializes the provided object using standard java serialization.
     */
    public Blob serialize(Object obj) {
      if (obj == null) {
        throw new NullPointerException("Object cannot be null.");
      }
      // No need to actually serialize byte arrays
      if (obj instanceof byte[]) {
        return new Blob((byte[]) obj);
      } else if (obj instanceof Byte[]) {
        return new Blob(PrimitiveArrays.toByteArray(Arrays.asList((Byte[]) obj)));
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = null;
      try {
        try {
          oos = new ObjectOutputStream(baos);
          oos.writeObject(obj);
          return new Blob(baos.toByteArray());
        } finally {
          try {
            baos.close();
          } finally {
            if (oos != null) {
              oos.close();
            }
          }
        }
      } catch (IOException ioe) {
        throw new NucleusException("Received IOException serializing object of type "
                                   + obj.getClass().getName(), ioe);
      }
    }

    /**
     * Deserialize the given blob using standard java deserialization.
     *
     * @throws NucleusException If <code>targetClass</code> does not extend
     * {@link Serializable} or the class of the result of deserialization does
     * not extend <code>targetClass</code>.
     */
    public Object deserialize(Blob blob, Class<?> targetClass) {
      if (blob == null) {
        throw new NullPointerException("Blob cannot be null.");
      }
      if (targetClass.equals(byte[].class)) {
        return blob.getBytes();
      } else if (targetClass.equals(Byte[].class)){
        byte[] bytes = blob.getBytes();
        return PrimitiveArrays.asList(bytes).toArray(new Byte[bytes.length]);
      }
      // If the bytes don't contain a valid java object we'll get
      // an exception.
      ByteArrayInputStream bais = new ByteArrayInputStream(blob.getBytes());
      ObjectInputStream ois = null;
      try {
        try {
          ois = new ObjectInputStream(bais);
          Object obj = ois.readObject();
          if (!targetClass.isAssignableFrom(obj.getClass())) {
            throw new NucleusException("Bytes in datastore comprise an object of type "
                + obj.getClass().getName() + " but expected type is " + targetClass.getName());
          }
          return obj;
        } finally {
          try {
            bais.close();
          } finally {
            if (ois != null) {
              ois.close();
            }
          }
        }
      } catch (IOException ioe) {
        throw new NucleusException("Received IOException deserializing a byte array.", ioe);
      } catch (ClassNotFoundException cnfe) {
        throw new NucleusException(
            "Received ClassNotFoundException deserializing a byte array.", cnfe);
      }
    }
  };

  /**
   * Transform the given value into a {@link Blob} using the serialization
   * strategy declared on the given member metadata, or using the default
   * serialization strategy if no serialization strategy is explicitly
   * declared.
   */
  Blob serialize(ClassLoaderResolver clr, AbstractMemberMetaData ammd, Object value) {
    SerializationStrategy serializationStrategy = getSerializationStrategy(clr, ammd);
    return serializationStrategy.serialize(value);
  }

  /**
   * Transform the given {@link Blob} into its deserialized form using the
   * serialization strategy declared on the given member metadata, or using
   * the default serialization strategy if no serialization strategy is
   * explicitly declared.
   */
  Object deserialize(ClassLoaderResolver clr, AbstractMemberMetaData ammd, Blob value) {
    SerializationStrategy serializationStrategy = getSerializationStrategy(clr, ammd);
    return serializationStrategy.deserialize(value, ammd.getType());
  }

  // visible for testing
  SerializationStrategy getSerializationStrategy(
      ClassLoaderResolver clr, AbstractMemberMetaData ammd) {
    ExtensionMetaData[] emdList = ammd.getExtensions();
    if (emdList != null) {
      // There is a hasExtension method on AbstractMemberMetaData
      // but it just loops over the entire list.  We might as well do it
      // ourselves and save the second pass.
      for (ExtensionMetaData emd : emdList) {
        if (emd.getKey().equals(SERIALIZATION_STRATEGY_KEY)) {
          Class<?> clazz = clr.classForName(emd.getValue());
          if (!SerializationStrategy.class.isAssignableFrom(clazz)) {
            throw new NucleusException("Custom serialization class " + emd.getValue()
                + " for member " + ammd.getFullFieldName() + " must implement "
                + SerializationStrategy.class);
          }
          try {
            return (SerializationStrategy) clazz.newInstance();
          } catch (InstantiationException e) {
            throw new NucleusException("Could not instantiate instance of " + clazz.getName(), e);
          } catch (IllegalAccessException e) {
            throw new NucleusException("Could not instantiate instance of " + clazz.getName(), e);
          }
        }
      }
    }
    // No serialization strategy explicitly configured so just use the default
    // impl.
    return DEFAULT_SERIALIZATION_STRATEGY;
  }
}
