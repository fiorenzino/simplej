package nz.fiore.vertx.test;

import io.reactivex.Single;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import nz.fiore.simplej.model.Whisky;
import nz.fiore.vertx.ext.jpa.JPAClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class JpaTest
{

   JsonObject config = new JsonObject()
            .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
            .put("driver_class", "org.hsqldb.jdbcDriver")
            .put("max_pool_size", 30);
   JPAClient jpaClient;
   Whisky whiskyP;
   Whisky whiskyP1;
   Whisky whiskyU;

   private Vertx vertx;

   @Before
   public void setUp(TestContext context)
   {
      vertx = Vertx.vertx();
      jpaClient = JPAClient.createShared(vertx, config);
      whiskyP = new Whisky();
      whiskyP.name = "flower";
      whiskyP.uuid = UUID.randomUUID().toString();

      whiskyP1 = new Whisky();
      whiskyP1.name = " uno flower";
      whiskyP1.uuid = UUID.randomUUID().toString();

      whiskyU = new Whisky(whiskyP.toJson());
      whiskyU.name = "flower UP";

   }

   @Test
   public void test(TestContext context)
   {
      Async async = context.async();
      // Connect to the database
      jpaClient.rxGetConnection().flatMap(conn -> {

         // Now chain some statements using flatmap composition
         Single<ResultSet> resa = conn.rxCreate(
                  "create table whiskies "
                           + " (uuid varchar(255), "
                           + "  name varchar(255),"
                           + "  collection_name varchar(255), "
                           + "  date datetime NULL, "
                           + "   amount decimal(19,4) )")
                  .flatMap(result1 -> conn.rxPersist("whiskies", whiskyP.toJson()))
                  .flatMap(result1 -> conn.rxPersist("whiskies", whiskyP1.toJson()))
                  .flatMap(result2 -> conn
                           .rxMerge("whiskies", whiskyU.toJson(), new JsonObject().put("uuid", whiskyU.uuid)))
                  .flatMap(result3 -> conn.rxQuery("select * from whiskies",
                           new JsonObject()))
                  .flatMap(result3 -> {
                     if (result3 != null && result3.getRows() != null)
                     {
                        result3.getRows().forEach(
                                 row -> {
                                    System.out.println(row);
                                 }
                        );
                     }
                     return conn.rxQuery("select * from whiskies where name like :NAME",
                              new JsonObject().put("NAME", "%uno%"));
                  })
                  .doOnError(error -> {
                     System.out.println("ERROR IN LIKE");
                     System.out.println(error.getCause());
                  }).doOnSuccess(success -> {
                     System.out.println("LIKE UNO QUERY");
                     if (success != null && success.getRows() != null)
                     {
                        success.getRows().forEach(
                                 row -> {
                                    System.out.println(row);
                                 }
                        );
                     }
                  });

         return resa.doAfterTerminate(conn::close);

      }).subscribe(resultSet -> {
         // Subscribe to the final result
         System.out.println("Results : " + resultSet.getResults());
         async.complete();
      }, err -> {
         System.out.println("Database problem");
         err.printStackTrace();
         async.complete();
      });
   }
}
