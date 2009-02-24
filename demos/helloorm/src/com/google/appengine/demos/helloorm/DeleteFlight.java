// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.demos.helloorm;

import java.io.IOException;

import javax.jdo.PersistenceManager;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DeleteFlight extends HttpServlet {

  private ServletConfig config;
  public void init(ServletConfig config) {
    this.config = config;
  }

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String key = req.getParameter("key");
    if (key == null) {
      resp.getWriter().println("No key provided.");
    }
    if (PersistenceStandard.get() == PersistenceStandard.JPA) {
      EntityManager em = EMF.emf.createEntityManager();
      EntityTransaction txn = em.getTransaction();
      try {
        txn.begin();
        Flight f = em.find(Flight.class, key);
        em.remove(f);
        txn.commit();
      } finally {
        if (txn.isActive()) {
          txn.rollback();
        }
        em.close();
      }
    } else {
      PersistenceManager pm = PMF.pmf.getPersistenceManager();
      try {
        Flight f = pm.getObjectById(Flight.class, key);
        pm.deletePersistent(f);
      } finally {
        if (pm.currentTransaction().isActive()) {
          pm.currentTransaction().rollback();
        }
        pm.close();
      }
    }
    resp.sendRedirect("/");
  }

}
