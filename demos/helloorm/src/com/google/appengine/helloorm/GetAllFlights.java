// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.jdo.PersistenceManager;
import javax.persistence.EntityManager;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

public class GetAllFlights extends HttpServlet {

  private ServletConfig config;
  public void init(ServletConfig config) {
    this.config = config;
  }

  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    long start = System.currentTimeMillis();
    PersistenceManager pm = null;
    EntityManager em = null;
    List<Flight> flights;
    try {
      if (PersistenceStandard.get() == PersistenceStandard.JPA) {
        em = EMF.emf.createEntityManager();
        flights = em.createQuery("select from " + Flight.class.getName()).getResultList();
      } else {
        pm = PMF.pmf.getPersistenceManager();
        flights = (List<Flight>)pm.newQuery(Flight.class).execute();
      }
      resp.getWriter().println("<table>");
      resp.getWriter().println("<tr>");
      resp.getWriter().println("<td>");
      resp.getWriter().println("</td>");
      resp.getWriter().println("<td>");
      resp.getWriter().println("Origin");
      resp.getWriter().println("</td>");
      resp.getWriter().println("<td>");
      resp.getWriter().println("Dest");
      resp.getWriter().println("</td>");
      resp.getWriter().println("</tr>");

      int index = 1;
      for (Flight f : flights) {
        resp.getWriter().println("<tr>");
        resp.getWriter().println("<td>");
        resp.getWriter().println("<a href=\"getFlight?key=" + f.getId() + "\"> " + index++ + "</a>");
        resp.getWriter().println("</td>");
        resp.getWriter().println("<td>");
        resp.getWriter().println(f.getOrig());
        resp.getWriter().println("</td>");
        resp.getWriter().println("<td>");
        resp.getWriter().println(f.getDest());
        resp.getWriter().println("</td>");
        resp.getWriter().println("</tr>");
      }
      resp.getWriter().println("</table>");
      resp.getWriter().println("<form action=\"addFlight\" method=\"post\">");
      resp.getWriter().println("<table>");
      resp.getWriter().println("<tr>");
      resp.getWriter().println("<th>Origin</th><td><input name=\"orig\" type=\"text\"/></td>");
      resp.getWriter().println("<th>Destination</th><td><input name=\"dest\" type=\"text\"/></td>");
      resp.getWriter().println("</tr>");
      resp.getWriter().println("<tr><td><input type=\"submit\" value=\"Add Flight\"></td></tr>");
      resp.getWriter().println("</table>");
      resp.getWriter().println("</form>");
      resp.getWriter().println("Request time in millis: " + (System.currentTimeMillis() - start));
      resp.getWriter().println("<br>");
      PersistenceStandard ps = PersistenceStandard.get();
      resp.getWriter().println("Persistence standard is " + ps.name());

      resp.getWriter().println("<form action=\"updatePersistenceStandard\" method=\"post\">");
      resp.getWriter().println("<input type=\"submit\" value=\"Switch to " + ps.getAlternate() + "\">");
      resp.getWriter().println(
          "<input name=\"persistenceStandard\" type=\"hidden\" value=\"" + ps.getAlternate() + "\"/>");
      resp.getWriter().println("</form>");
    } finally {
      if (pm != null) {
        pm.close();
      }

      if (em != null) {
        em.close();
      }
    }
  }

}
