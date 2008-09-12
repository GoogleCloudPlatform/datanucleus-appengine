// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.EntityNotFoundException;
import com.google.apphosting.api.datastore.Key;

import org.datanucleus.test.HasTableAndColumnsInMappingJDO;
import org.datanucleus.test.Flight;

import java.util.Properties;

import javax.jdo.JDOHelper;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOTableAndColumnTest extends JDOTestCase {

  public void testInsert() throws EntityNotFoundException {
    HasTableAndColumnsInMappingJDO htacim = new HasTableAndColumnsInMappingJDO();
    htacim.setFoo("foo val");
    pm.makePersistent(htacim);
    assertNotNull(htacim.getId());
    Entity entity = ldth.ds.get(KeyFactory.decodeKey(htacim.getId()));
    assertNotNull(entity);
    assertEquals(HasTableAndColumnsInMappingJDO.TABLE_NAME, entity.getKind());
    assertEquals("foo val", entity.getProperty(HasTableAndColumnsInMappingJDO.FOO_COLUMN_NAME));
  }

  public void testInsert_TablePrefix() throws EntityNotFoundException {
    pm.close();
    pmf.close();
    Properties properties = new Properties();
    properties.setProperty("javax.jdo.PersistenceManagerFactoryClass",
                    "org.datanucleus.jdo.JDOPersistenceManagerFactory");
    properties.setProperty("javax.jdo.option.ConnectionURL","appengine");
    properties.setProperty("datanucleus.NontransactionalRead", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.NontransactionalWrite", Boolean.TRUE.toString());
    properties.setProperty("datanucleus.identifier.wordSeparator", "___");
    properties.setProperty("datanucleus.identifier.tablePrefix", "PREFIX");
    properties.setProperty("datanucleus.identifier.tableSuffix", "SUFFIX");
    pmf = JDOHelper.getPersistenceManagerFactory(properties);
    pm = pmf.getPersistenceManager();

    Flight f1 = new Flight();
    f1.setOrigin("BOS");
    f1.setDest("MIA");
    f1.setMe(2);
    f1.setYou(4);
    f1.setName("Harold");
    pm.makePersistent(f1);
    Entity entity = ldth.ds.get(KeyFactory.decodeKey(f1.getId()));
    assertNotNull(entity);
    assertEquals("PREFIX" + Flight.class.getSimpleName().toUpperCase() + "SUFFIX", entity.getKind());
    assertEquals("Harold", entity.getProperty("NAME"));
  }

  public void testFetch() {
    Entity entity = new Entity(HasTableAndColumnsInMappingJDO.TABLE_NAME);
    entity.setProperty(HasTableAndColumnsInMappingJDO.FOO_COLUMN_NAME, "foo val");
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.encodeKey(key);
    HasTableAndColumnsInMappingJDO htacim =
        pm.getObjectById(HasTableAndColumnsInMappingJDO.class, keyStr);
    assertNotNull(htacim);
    assertEquals(keyStr, htacim.getId());
    assertEquals("foo val", htacim.getFoo());
  }
}
