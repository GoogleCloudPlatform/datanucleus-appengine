// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.jdo.PersistenceManager;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class AddFlight extends HttpServlet {

  private ServletConfig config;
  public void init(ServletConfig config) {
    this.config = config;
  }

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String orig = req.getParameter("orig");
    String dest = req.getParameter("dest");
    Flight f = new Flight(orig, dest);
    if (PersistenceStandard.get() == PersistenceStandard.JPA) {
      EntityManager em = EMF.emf.createEntityManager();
      try {
        EntityTransaction txn = em.getTransaction();
        txn.begin();
        em.persist(f);
        txn.commit();
      } finally {
        em.close();
      }
    } else {
      PersistenceManager pm = PMF.pmf.getPersistenceManager();
      try {
        pm.makePersistent(f);
      } finally {
        pm.close();
      }
    }
    resp.sendRedirect("/");
  }
}
