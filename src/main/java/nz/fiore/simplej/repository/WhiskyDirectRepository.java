package nz.fiore.simplej.repository;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jpa.JPAClient;
import io.vertx.ext.jpa.sql.JPAConnection;
import io.vertx.ext.jpa.util.RestrinctionHandler;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;

/**
 * Created by fiorenzo on 16/04/17.
 */
public class WhiskyDirectRepository
{

   static String TABLE_NAME = "whiskies";
   public JPAClient jpaClient;
   protected Logger logger = LoggerFactory.getLogger(getClass());

   public WhiskyDirectRepository()
   {
   }

   public WhiskyDirectRepository(Vertx vertx, JsonObject config)
   {
      this.jpaClient = JPAClient.createShared(vertx, new JsonObject()
               .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
               .put("driver_class", "org.hsqldb.jdbcDriver")
               .put("max_pool_size", 30));
   }

   public void init(Future<Void> future)
   {
      System.out.println("init");

      jpaClient.getConnection(connection -> {
         connection.result()
                  .execute("create table IF NOT EXISTS whiskies" +
                                    " (uuid varchar(255), " +
                                    " name varchar(255)," +
                                    " collection_name varchar(255)," +
                                    " date datetime NULL," +
                                    " amount decimal(19,4))",
                           future.completer());
      });
   }

   public void persist(JsonObject params, Handler<AsyncResult<UpdateResult>> handler)
   {
      jpaClient.persist(TABLE_NAME, params, handler);
   }

   public void merge(JsonObject params, JsonObject key, Handler<AsyncResult<UpdateResult>> handler)
   {
      jpaClient.merge(TABLE_NAME, params, key, handler);
   }

   public void update(JsonObject params, JsonObject key, Handler<AsyncResult<UpdateResult>> handler)
   {
      jpaClient.merge(TABLE_NAME, params, key, handler);
   }

   public void delete(JsonObject key, Handler<AsyncResult<UpdateResult>> handler)
   {
      jpaClient.delete(TABLE_NAME, key, handler);
   }

   public void list(JsonObject params, Handler<AsyncResult<ResultSet>> handler)
   {
      RestrinctionHandler<JsonObject, String, StringBuffer> rh = new RestrinctionHandler<JsonObject, String, StringBuffer>()
      {
         String alias = "a";
         String separator = " WHERE ";

         @Override public void handle(JsonObject params, String table, StringBuffer toSql)
         {
            toSql.append("select * from " + table + " " + alias + " ");
            if (params.getString("name") != null)
            {
               toSql.append(separator).append(alias).append(".name LIKE :name ");
               params.put("name", "%" + params.getString("name") + "%");
               separator = " and ";
            }
             if (params.getString("collection_name") != null)
            {
               toSql.append(separator).append(alias).append(".collection_name LIKE :collection_name ");
               params.put("collection_name", "%" + params.getString("collection_name") + "%");
               separator = " and ";
            }
            if (params.getString("uuid") != null)
            {
               toSql.append(separator).append(alias).append(".uuid = :uuid ");
               separator = " and ";
            }
         }
      };
      jpaClient.query(TABLE_NAME, params, rh, handler);
   }

   public void collections(JsonObject params, Handler<AsyncResult<ResultSet>> handler)
   {
      jpaClient.query("select * from " + TABLE_NAME + " WHERE collection_name = :collection_name ", params, handler);
   }

}
