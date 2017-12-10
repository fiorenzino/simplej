package nz.fiore.simplej.verticle;

import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import nz.fiore.vertx.ext.jpa.JPAClient;

public class RxVerticle extends AbstractVerticle
{

   JsonObject config = new JsonObject()
            .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
            .put("driver_class", "org.hsqldb.jdbcDriver")
            .put("max_pool_size", 30);

   JPAClient jpaClient = JPAClient.createShared(vertx, config);

   @Override public void start() throws Exception
   {

      // Connect to the database
      jpaClient.rxGetConnection().flatMap(conn -> {

         // Now chain some statements using flatmap composition
         Single<io.vertx.ext.sql.ResultSet> resa = conn.rxCreate(
                  "create table IF NOT EXISTS whiskies "
                           + " (uuid varchar(255), "
                           + "  name varchar(255),"
                           + "  collection_name varchar(255), "
                           + "  date datetime NULL, "
                           + "   amount decimal(19,4)")
                  .flatMap(result1 -> conn.rxPersist("whiskies", new JsonObject()))
                  .flatMap(result2 -> conn.rxMerge("whiskies", new JsonObject(), new JsonObject()))
                  .flatMap(result3 -> conn.rxQuery("whiskies", new JsonObject()))
                  .doOnError(error -> {
                     System.out.println(error.getCause());
                  }).doOnSuccess(success -> {
                     System.out.println(success.getResults());
                  });

         return resa.doAfterTerminate(conn::close);

      }).subscribe(resultSet -> {
         // Subscribe to the final result
         System.out.println("Results : " + resultSet.getResults());
      }, err -> {
         System.out.println("Database problem");
         err.printStackTrace();
      });
   }
}
