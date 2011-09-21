package com.google.appengine.datanucleus.bugs.jpa;

import java.util.UUID;

import javax.persistence.EntityManager;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.bugs.test.Issue167Child;
import com.google.appengine.datanucleus.bugs.test.Issue167Parent;

public class Issue167Test extends JPABugTestCase {
  public void testRun() {
    Issue167Parent p = new Issue167Parent(UUID.randomUUID().toString());

    EntityManager em = emf.createEntityManager();
    try {
      p.getChildren().add(new Issue167Child("1"));
      p.getChildren().add(new Issue167Child("2"));
      p.getChildren().add(new Issue167Child("3"));

      em.getTransaction().begin();
      em.persist(p);
      em.getTransaction().commit();

      NucleusLogger.GENERAL.debug(">> Parent " + p.getName() + " stored succesfully");
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in persist", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }

    for (Issue167Child c : p.getChildren()) {
      NucleusLogger.GENERAL.debug("Parent contains child " + c.getData());
    }

    p.getChildren().remove(1); // "2"
    p.getChildren().remove(1); // "3"
    p.getChildren().remove(0); // "1"
    NucleusLogger.GENERAL.debug("Children removed, now contains " + p.getChildren().size() + " children");

    em = emf.createEntityManager();
    try {
      em.getTransaction().begin();
      p = em.merge(p);
      em.getTransaction().commit();

      NucleusLogger.GENERAL.debug(">> Parent " + p.getName()
          + " stored succesfully with " + p.getChildren().size() + " children");
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in merge", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }

    em = emf.createEntityManager();

    try {
      em.getTransaction().begin();
      p = (Issue167Parent) em.createQuery(
          "select p from " + Issue167Parent.class.getName() + " p where p.name='"
          + p.getName() + "'").getResultList().get(0);
      p.getChildren(); // Touch children field to get it detached
      em.getTransaction().commit();

      NucleusLogger.GENERAL.debug(">> Parent " + p.getName() + " fetched succesfully");
    } catch (Exception e) {
      NucleusLogger.GENERAL.error("Exception in fetch", e);
      fail("Exception in test : " + e.getMessage());
      return;
    } finally {
      if (em.getTransaction().isActive()) {
        em.getTransaction().rollback();
      }
      em.close();
    }

    if (p.getChildren().size() > 0) {
      fail("Should not have any children but has " + p.getChildren().size());
    }
  }
}
