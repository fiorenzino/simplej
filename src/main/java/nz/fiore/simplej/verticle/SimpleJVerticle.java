package nz.fiore.simplej.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import nz.fiore.simplej.model.Whisky;
import nz.fiore.simplej.repository.WhiskyDirectRepository;
import nz.fiore.simplej.utils.HandlerUtils;

/**
 * Created by fiorenzo on 16/04/17.
 */
public class SimpleJVerticle extends AbstractVerticle
{

   WhiskyDirectRepository whiskyRepository;

   @Override
   public void start(Future<Void> startFuture) throws Exception
   {

      whiskyRepository = new WhiskyDirectRepository(vertx, config());
      // Create a router object.
      Router router = Router.router(vertx);

      // Bind "/" to our hello message - so we are still compatible.
      router.route("/").handler(routingContext -> {
         HttpServerResponse response = routingContext.response();
         response
                  .putHeader("content-type", "text/html")
                  .end("<h1>Hello from my first Vert.x 3 application</h1>");
      });

      router.route("/api*").handler(BodyHandler.create());
      router.post("/api/whiskies").handler(this::create);
      router.delete("/api/whiskies/:uuid").handler(this::delete);
      router.put("/api/whiskies/:uuid").handler(this::merge);
      router.get("/api/whiskies").handler(this::list);
      router.get("/api/collections").handler(this::collections);

      // Create the HTTP server and pass the "accept" method to the request handler.
      Future<Void> future = Future.future();
      future.setHandler(rex -> {
         if (rex.succeeded())
         {
            vertx
                     .createHttpServer()
                     .requestHandler(router::accept)
                     .listen(8080,
                              result -> {
                                 if (result.succeeded())
                                 {
                                    startFuture.complete();
                                 }
                                 else
                                 {
                                    startFuture.fail(result.cause());
                                 }
                              }
                     );
         }
         else
         {

         }
      });
      whiskyRepository.init(future);

   }

   private void merge(RoutingContext routingContext)
   {
      System.out.println("merge");

      String uuid = routingContext.request().getParam("uuid");
      Whisky whisky = new Whisky(routingContext.getBodyAsJson());
      if (uuid == null)
      {
         routingContext.response().setStatusCode(400).end();
      }
      else
      {
         whiskyRepository.update(whisky.toJson(), new JsonObject().put("uuid", uuid),
                  HandlerUtils.handleUpdateResult(routingContext, whisky));
      }

   }

   private void delete(RoutingContext routingContext)
   {
      System.out.println("delete");

      String uuid = routingContext.request().getParam("uuid");
      if (uuid == null)
      {
         routingContext.response().setStatusCode(400).end();
      }
      else
      {
         whiskyRepository.delete(new JsonObject().put("uuid", uuid), handle -> {
            if (handle.succeeded())
            {
               routingContext.response().setStatusCode(204).end();
            }
            else
            {
               routingContext.response().setStatusCode(400).end();
            }
         });
      }

   }

   private void create(RoutingContext routingContext)
   {
      System.out.println("create");

      Whisky whisky = new Whisky(routingContext.getBodyAsJson());
      if (whisky == null)
      {
         routingContext.response().setStatusCode(400).end();
      }
      else
      {
         whiskyRepository.persist(whisky.toJson(), HandlerUtils.handleUpdateResult(routingContext, whisky));
      }
   }

   private void list(RoutingContext routingContext)
   {
      System.out.println("list");
      JsonObject params = new JsonObject();
      String uuid = routingContext.request().getParam("uuid");
      params.put("uuid", uuid);
      String name = routingContext.request().getParam("name");
      params.put("name", name);
      whiskyRepository.list(params, HandlerUtils.handleResultSet(routingContext));
   }

   private void collections(RoutingContext routingContext)
   {
      System.out.println("collections");
      JsonObject params = new JsonObject();
      String collection_name = routingContext.request().getParam("collection_name");
      params.put("collection_name", collection_name);
      if (collection_name == null)
      {
         routingContext.response().setStatusCode(400).end();
      } else {
         whiskyRepository.collections(params, HandlerUtils.handleResultSet(routingContext));
      }
   }
}
