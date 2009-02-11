// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;

import org.datanucleus.test.HasEnumJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class TypeConversionUtilsTest extends TestCase {
  public void testNullToArray() {
    String[] stringArray =
        (String[]) TypeConversionUtils.convertDatastoreListToPojoArray(null, String.class);

    HasEnumJDO.MyEnum[] enumArray = (HasEnumJDO.MyEnum[])
        TypeConversionUtils.convertDatastoreListToPojoArray(null, HasEnumJDO.MyEnum.class);

    int[] intArray =
        (int[]) TypeConversionUtils.convertDatastoreListToPojoArray(null, Integer.TYPE);

  }
}