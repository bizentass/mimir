package mimir.experiments;

import java.io._
import scala.collection.JavaConversions._
import org.specs2.mutable._

import mimir._;
import mimir.util._;

/**
 * Evaluation based on "MCDB: A Monte Carlo Approach to Managing 
 * Uncertain Data" by Jampani et. al. (SIGMOD '08)
 */

object MCDBQueries
  extends SQLTestSpecification("MCDBExperiments", Map( 
    "jdbc" -> "sqlite-inline"
  ))
{
  
  val productDataFile = new File("test/data/Product.sql");
  val reviewDataFiles = List(
      new File("test/data/ratings1.csv"),
      new File("test/data/ratings2.csv")
    )

  sequential

  "Mimir" should {
    "Be able to load TPC-H" >> {
      stmts(productDataFile).map( update(_) )
      db.loadTable(reviewDataFiles(0))
      db.loadTable(reviewDataFiles(1))
      db.backend.resultRows("SELECT * FROM PRODUCT;") must have size(6)
    }

    "Query 1" >> {
      // CREATE VIEW from_japan AS
      //   SELECT *
      //   FROM nation, supplier, lineitem, partsupp
      //   WHERE n_name=’JAPAN’ AND s_suppkey=ps_suppkey AND
      //         ps_partkey=l_partkey AND ps_suppkey=l suppkey AND
      //         n_nationkey = s_nationkey
      //
      // CREATE VIEW increase_per_cust AS
      //   SELECT o_custkey AS custkey, 
      //          SUM(yr(o_orderdate)-1994.0) / 
      //              SUM(1995.0-yr(o_orderdate)) AS incr 
      //   FROM ORDERS
      //   WHERE yr(o_orderdate)=1994 OR 
      //         yr(o_orderdate)=1995 GROUP BY o_custkey
      //
      // CREATE TABLE order_increase AS 
      //   FOR EACH o in ORDERS
      //     WITH temptable AS Poisson(
      //       SELECT incr
      //      FROM increase_per_cust 
      //      WHERE o_custkey=custkey AND yr(o_orderdate)=1995
      //    )
      //    SELECT t.value AS new cnt, o_orderkey FROM temptable t
      //
      // SELECT SUM(newRev-oldRev)
      //   FROM (
      //     SELECT l_extendedprice*(1.0-l_discount)*new_cnt AS newRev, 
      //           (l_extendedprice*(1.0-l_discount)) AS oldRev
      //     FROM increase_per_cust, from_japan 
      //     WHERE l_orderkey=o orderkey
      //   )
    }

    "Query 2" >> {
      // CREATE VIEW orders_today AS
      //   SELECT *
      //   FROM orders, lineitem
      //   WHERE o_orderdate=today AND o_orderkey=l_orderkey
      //
      // CREATE VIEW params AS
      //   SELECT AVG(l_shipdate-o_orderdate) AS ship_mu,
      //          AVG(l_receiptdate-l_shipdate) AS arrv_mu,
      //          STDDEV(l_shipdate-o_orderdate) AS ship_sigma, 
      //          STD_DEV(l_receiptdate-l_shipdate) AS arrv_sigma, 
      //          l_partkey AS p_partkey
      //   FROM orders, lineitem 
      //   WHERE o_orderkey=l_orderkey GROUP BY l_partkey
      //
      // CREATE TABLE ship_durations AS 
      //   FOR EACH o in orders_today
      //     WITH gamma ship AS DiscGamma( 
      //       SELECT ship_mu, ship_sigma FROM params
      //       WHERE p_partkey=l_partkey
      //     )
      //     WITH gamma_arrv AS DiscGamma( 
      //       SELECT arrv_mu, arrv_sigma FROM params
      //       WHERE p_partkey=l_partkey
      //     )
      //   SELECT gs.value AS ship, ga.value AS arrv 
      //   FROM gamma ship gs, gamma arrv ga
      //
      // SELECT MAX(ship+arrv) FROM ship durations
    }

    "Query 3" >> {
      // CREATE TABLE prc hist(ph month, ph year, ph prc, ph partkey) AS
      //   FOR EACH ps in partsupp
      //     WITH time_series AS RandomWalk(
      //       VALUES (ps_supplycost,12,"Dec",1995,-0.02,0.04)
      //     )
      //     SELECT month, year, value, ps_partkey
      //     FROM time_series ts
      //
      // SELECT MIN(ph_prc) AS min_prc, ph_month, ph_year, ph_partkey
      // FROM prc_hist
      // GROUP BY ph_month, ph_year, ph_partkey
      // 
      // SELECT SUM(min prc*l_quantity)
      // FROM prc_hist, lineitem, orders
      // WHERE ph_month=month(o_orderdate) AND l_orderkey=
      //       o_orderkey AND yr(o_orderdate)=1995 AND ph_partkey=l_partkey
    }

    "Query 4" >> {
      // CREATE VIEW params AS
      //   SELECT 2.0 AS p0shape, 
      //          1.333*AVG(l_extendedprice*(1.0-l_discount)) AS p0scale, 
      //          2.0 AS d0shape, 
      //          4.0*AVG(l_quantity) AS d0scale, 
      //          l_partkey AS p_partkey
      //   FROM lineitem l 
      //   GROUP BY l_partkey
      //
      // CREATE TABLE demands (new_dmnd, old_dmnd, old_prc, new_prc, nd_partkey, nd_suppkey) AS 
      //   FOR EACH l IN (
      //       SELECT * FROM lineitem, orders
      //       WHERE l orderkey=o orderkey AND yr(o orderdate)=1995
      //     ) 
      //     WITH new dmnd AS Bayesian (
      //       ( SELECT p0shape, p0scale, d0shape, d0scale FROM params
      //         WHERE l_1partkey = p_partkey ),
      //       (VALUES (l_quantity, 
      //                l_extendedprice*(1.0- l_discount))/l_quantity, 
      //                l_extendedprice* 1.05*(1.0-l_discount)/l_quantity))
      //     SELECT nd.value, l_quantity, 
      //            l_extendedprice* (1.0-l_discount))/ l_quantity, 1.05*
      //              l_extendedprice*(1.0-l_discount)/l_quantity, l_partkey, l_suppkey
      //     FROM new_dmnd nd
      //
      // SELECT SUM (new_prf-old_prf) FROM (
      //   SELECT new_dmnd*(new_prc-ps_supplycost) AS new_prf,
      //          old_dmnd*(old_prc-ps_supplycost) AS old_prf
      //   FROM partsupp, demands
      //   WHERE ps_partkey=nd_partkey AND
      //         ps_suppkey=nd_suppkey
      // )
    }
  }

}