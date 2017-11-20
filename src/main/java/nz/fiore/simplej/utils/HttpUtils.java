package nz.fiore.simplej.utils;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by fiorenzo on 24/03/17.
 */
public class HttpUtils {

    public static void end(RoutingContext routingContext, Object object, int status) {
        endObj(routingContext, object, status);
    }

    public static void end200(RoutingContext routingContext, Object object) {
        endObj(routingContext, object, 200);
    }

    public static void end201(RoutingContext routingContext) {
        endMsg(routingContext, null, 201);
    }

    public static void end204(RoutingContext routingContext, String msg) {
        endMsg(routingContext, msg, 204);
    }

    public static void end400(RoutingContext routingContext, String msg) {
        endMsg(routingContext, msg, 400);
    }

    public static void badRequest(RoutingContext routingContext, Throwable ex) {
        ex.printStackTrace();
        endMsg(routingContext, ex.getMessage(), 400);
    }


    public static void end403(RoutingContext routingContext, String msg) {
        endMsg(routingContext, msg, 403);
    }

    public static void end404(RoutingContext routingContext, String msg) {
        endMsg(routingContext, msg, 404);
    }

    public static void notFound(RoutingContext routingContext) {
        endMsg(routingContext, "not_found", 404);
    }

    public static void end500(RoutingContext routingContext, String msg) {
        endMsg(routingContext, msg, 500);
    }

    public static void internalError(RoutingContext routingContext, Throwable ex) {
        ex.printStackTrace();
        endMsg(routingContext, ex.getMessage(), 500);
    }


    public static void notImplemented(RoutingContext routingContext) {
        endMsg(routingContext, "not_implemented", 501);
    }

    public static void badGateway(RoutingContext routingContext, Throwable ex) {
        ex.printStackTrace();
        endMsg(routingContext, "bad_gateway: " + ex.getMessage(), 502);
    }

    public static void serviceUnavailable(RoutingContext routingContext) {
        endMsg(routingContext, "error: ", 503);
    }

    public static void serviceUnavailable(RoutingContext routingContext, Throwable ex) {
        ex.printStackTrace();
        endMsg(routingContext, "error: " + ex.getMessage(), 503);
    }

    public static void serviceUnavailable(RoutingContext routingContext, String cause) {
        endMsg(routingContext, "error: " + cause, 503);
    }

    public static void endMsg(RoutingContext routingContext, String msg, int status) {
        HttpServerResponse response = routingContext.response()
                .setStatusCode(status);
        if (msg != null)
            response.setStatusMessage(msg);
        HttpUtils.allowOrigin(routingContext, response).end();
    }

    public static void endObj(RoutingContext routingContext, Object object, int status) {
        HttpServerResponse response = routingContext.response()
                .putHeader("content-type",
                        "application/json; charset=utf-8")
                .setStatusCode(status);
        if (object != null) {
            allowOrigin(routingContext, response)
                    .end(Json.encodePrettily(object));
        } else {
            HttpUtils.allowOrigin(routingContext, response).end();
        }

    }


    /**
     * Enable CORS support.
     *
     * @param router router instance
     */
    public static void enableCorsSupport(Router router) {
        Set<String> allowHeaders = new HashSet<>();
        allowHeaders.add("x-requested-with");
        allowHeaders.add("Access-Control-Allow-Origin");
        allowHeaders.add("origin");
        allowHeaders.add("Content-Type");
        allowHeaders.add("accept");
        Set<HttpMethod> allowMethods = new HashSet<>();
        allowMethods.add(HttpMethod.GET);
        allowMethods.add(HttpMethod.POST);
        allowMethods.add(HttpMethod.DELETE);
        allowMethods.add(HttpMethod.PATCH);

        router.route().handler(CorsHandler.create("*")
                .allowedHeaders(allowHeaders)
                .allowedMethods(allowMethods));
    }

    public static HttpServerResponse allowOrigin(RoutingContext routingContext, HttpServerResponse response) {
        routingContext.response().putHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, PATCH, OPTIONS");
        routingContext.response().putHeader("Access-Control-Allow-Credentials", "true");
        routingContext.response().putHeader("Access-Control-Allow-Headers",
                "Origin, x-requested-with,  X-Requested-With, Content-Type, Accept, Authorization, accept, authorization");
        if (routingContext.request().getHeader("Origin") != null) {
            response.putHeader("Access-Control-Allow-Origin",
                    routingContext.request().getHeader("Origin"));
        } else {
            response.putHeader("Access-Control-Allow-Origin", "*");
        }
        return response;
    }

    public static void options(RoutingContext routingContext) {
        HttpServerResponse response = routingContext.response()
                .setStatusCode(200);
        allowOrigin(routingContext, response).end();
    }

    public static void listResult(RoutingContext routingContext, int startRow, int pageSize, int listSize, String jsonEncoded) {
        HttpServerResponse response = routingContext.response()
                .putHeader("content-type",
                        "application/json; charset=utf-8")
                .putHeader("Access-Control-Expose-Headers", "startRow, pageSize, listSize, startRow")
                .putHeader("pageSize", "" + pageSize)
                .putHeader("listSize", "" + listSize)
                .putHeader("startRow", "" + startRow);
        allowOrigin(routingContext, response).end(jsonEncoded);
    }


}
