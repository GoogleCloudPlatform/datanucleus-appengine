// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.Book;
import org.datanucleus.test.HasMultiValuePropsJPA;
import org.datanucleus.test.HasVersionJPA;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.jdo.JDOHelper;
import javax.persistence.OptimisticLockException;
import javax.persistence.RollbackException;

/**
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
public class JPAUpdateTest extends JPATestCase {

  private static final String DEFAULT_VERSION_PROPERTY_NAME = "VERSION";

  public void testUpdateAfterFetch() throws EntityNotFoundException {
    Key key = ldth.ds.put(Book.newBookEntity("jimmy", "12345", "the title"));

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    Book book = em.find(Book.class, keyStr);

    assertEquals(keyStr, book.getId());
    assertEquals("jimmy", book.getAuthor());
    assertEquals("12345", book.getIsbn());
    assertEquals("the title", book.getTitle());

    book.setIsbn("56789");
    commitTxn();

    Entity bookCheck = ldth.ds.get(key);
    assertEquals("jimmy", bookCheck.getProperty("author"));
    assertEquals("56789", bookCheck.getProperty("isbn"));
    assertEquals("the title", bookCheck.getProperty("title"));
  }

  public void testUpdateAfterSave() throws EntityNotFoundException {
    Book b = new Book();
    b.setAuthor("max");
    b.setIsbn("22333");
    b.setTitle("yam");

    beginTxn();
    em.persist(b);
    commitTxn();

    assertNotNull(b.getId());

    beginTxn();
    b.setTitle("not yam");
    em.merge(b);
    commitTxn();

    Entity bookCheck = ldth.ds.get(KeyFactory.stringToKey(b.getId()));
    assertEquals("max", bookCheck.getProperty("author"));
    assertEquals("22333", bookCheck.getProperty("isbn"));
    assertEquals("not yam", bookCheck.getProperty("title"));
  }

  public void testOptimisticLocking_Update() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(HasVersionJPA.class, keyStr);
    hv.setValue("value");
    commitTxn();
    assertEquals(2L, hv.getVersion());
    // make sure the version gets bumped
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 3L);

    beginTxn();
    hv = em.find(HasVersionJPA.class, keyStr);
    hv.setValue("another value");
    // we update the entity directly in the datastore right before commit
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 7L);
    ldth.ds.put(entity);
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    // make sure the version didn't change on the model object
    assertEquals(2L, JDOHelper.getVersion(hv));
  }

  public void testOptimisticLocking_Delete() {
    switchDatasource(EntityManagerFactoryName.nontransactional_ds_non_transactional_ops_not_allowed);
    Entity entity = new Entity(HasVersionJPA.class.getSimpleName());
    entity.setProperty(DEFAULT_VERSION_PROPERTY_NAME, 1L);
    Key key = ldth.ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasVersionJPA hv = em.find(HasVersionJPA.class, keyStr);

    // delete the entity in the datastore right before we commit
    ldth.ds.delete(key);
    hv.setValue("value");
    try {
      commitTxn();
      fail("expected optimistic exception");
    } catch (RollbackException re) {
      // good
      assertTrue(re.getCause() instanceof OptimisticLockException);
    }
    // make sure the version didn't change on the model object
    assertEquals(1L, JDOHelper.getVersion(hv));
  }

  public void testUpdateList_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    List<String> list = Utils.newArrayList("a", "b");
    pojo.setStrList(list);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrList().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strList")).size());
  }

  public void testUpdateList_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    List<String> list = Utils.newArrayList("a", "b");
    pojo.setStrList(list);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    list = Utils.newArrayList("a", "b", "zoom");
    pojo.setStrList(list);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strList")).size());
  }

  public void testUpdateArrayList_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    ArrayList<String> list = Utils.newArrayList("a", "b");
    pojo.setStrArrayList(list);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrArrayList().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strArrayList")).size());
  }

  public void testUpdateArrayList_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    ArrayList<String> list = Utils.newArrayList("a", "b");
    pojo.setStrArrayList(list);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    list = Utils.newArrayList("a", "b", "zoom");
    pojo.setStrArrayList(list);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strArrayList")).size());
  }

  public void testUpdateLinkedList_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    LinkedList<String> list = Utils.newLinkedList("a", "b");
    pojo.setStrLinkedList(list);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrLinkedList().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strLinkedList")).size());
  }

  public void testUpdateLinkedList_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    LinkedList<String> list = Utils.newLinkedList("a", "b");
    pojo.setStrLinkedList(list);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    list = Utils.newLinkedList("a", "b", "zoom");
    pojo.setStrLinkedList(list);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strLinkedList")).size());
  }

  public void testUpdateSet_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    Set<String> set = Utils.newHashSet("a", "b");
    pojo.setStrSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrSet().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strSet")).size());
  }

  public void testUpdateSet_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    Set<String> set = Utils.newHashSet("a", "b");
    pojo.setStrSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    set = Utils.newHashSet("a", "b", "zoom");
    pojo.setStrSet(set);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strSet")).size());
  }

  public void testUpdateHashSet_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    HashSet<String> set = Utils.newHashSet("a", "b");
    pojo.setStrHashSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrHashSet().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strHashSet")).size());
  }

  public void testUpdateHashSet_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    HashSet<String> set = Utils.newHashSet("a", "b");
    pojo.setStrHashSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    set = Utils.newHashSet("a", "b", "zoom");
    pojo.setStrHashSet(set);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strHashSet")).size());
  }

  public void testUpdateLinkedHashSet_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    LinkedHashSet<String> set = Utils.newLinkedHashSet("a", "b");
    pojo.setStrLinkedHashSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrLinkedHashSet().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strLinkedHashSet")).size());
  }

  public void testUpdateLinkedHashSet_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    LinkedHashSet<String> set = Utils.newLinkedHashSet("a", "b");
    pojo.setStrLinkedHashSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    set = Utils.newLinkedHashSet("a", "b", "zoom");
    pojo.setStrLinkedHashSet(set);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strLinkedHashSet")).size());
  }

  public void testUpdateTreeSet_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    TreeSet<String> set = Utils.newTreeSet("a", "b");
    pojo.setStrTreeSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrTreeSet().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strTreeSet")).size());
  }

  public void testUpdateTreeSet_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    TreeSet<String> set = Utils.newTreeSet("a", "b");
    pojo.setStrTreeSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    set = Utils.newTreeSet("a", "b", "zoom");
    pojo.setStrTreeSet(set);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strTreeSet")).size());
  }

  public void testUpdateSortedSet_Add() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    SortedSet<String> set = Utils.newTreeSet("a", "b");
    pojo.setStrSortedSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    pojo.getStrSortedSet().add("zoom");
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strSortedSet")).size());
  }

  public void testUpdateSortedSet_Reset() throws EntityNotFoundException {
    HasMultiValuePropsJPA pojo = new HasMultiValuePropsJPA();
    SortedSet<String> set = Utils.newTreeSet("a", "b");
    pojo.setStrSortedSet(set);
    beginTxn();
    em.persist(pojo);
    commitTxn();
    em.close();
    em = emf.createEntityManager();
    beginTxn();
    pojo = em.find(HasMultiValuePropsJPA.class, pojo.getId());
    set = Utils.newTreeSet("a", "b", "zoom");
    pojo.setStrSortedSet(set);
    commitTxn();
    Entity e = ldth.ds.get(KeyFactory.stringToKey(pojo.getId()));
    assertEquals(3, ((List<?>)e.getProperty("strSortedSet")).size());
  }


}
