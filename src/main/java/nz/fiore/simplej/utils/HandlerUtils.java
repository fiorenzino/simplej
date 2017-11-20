package nz.fiore.simplej.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.RoutingContext;

import java.util.function.Supplier;

/**
 * Created by fiorenzo on 24/03/17.
 */
public class HandlerUtils {

    static Logger logger = LoggerFactory.getLogger(HandlerUtils.class);

    public static Handler<AsyncResult<UpdateResult>> handleUpdateResult(RoutingContext routingContext, Object obj) {
        return ar -> {
            if (ar.succeeded()) {
                HttpUtils.end200(routingContext, obj);
            } else {
                logger.error("handleUpdateResult failed!", ar.cause());
                HttpUtils.end500(routingContext, ar.cause().getMessage());
            }
        };
    }

    public static Handler<AsyncResult<UpdateResult>> handleUpdateResult(RoutingContext routingContext) {
        return ar -> {
            if (ar.succeeded()) {
                HttpUtils.end200(routingContext, ar.result().toJson());
            } else {
                logger.error("handleUpdateResult failed!", ar.cause());
                HttpUtils.end500(routingContext, ar.cause().getMessage());
            }
        };
    }

    public static Handler<AsyncResult<ResultSet>> handleResultSet(RoutingContext routingContext) {
        return ar -> {
            if (ar.succeeded()) {
                Object object = null;
                if (ar.result() != null && ar.result().getRows() != null && ar.result().getRows().size() > 0) {
                    object = ar.result().getRows();
                }
                HttpUtils.end200(routingContext, object);
            } else {
                logger.error("handleResultSet failed!", ar.cause());
                HttpUtils.end500(routingContext, ar.cause().getMessage());
            }
        };
    }




    public static Handler<AsyncResult<Void>> deleteResultHandler(RoutingContext routingContext) {
        return res -> {
            if (res.succeeded()) {
                HttpUtils.end204(routingContext, "delete_success");
            } else {
                logger.error("failed!", res.cause());
                HttpUtils.internalError(routingContext, res.cause());

            }
        };
    }

    public static Handler<AsyncResult<Void>> deleteResultHandlerClose(RoutingContext routingContext,
                                                                      SQLConnection sqlConnection) {
        return res -> {
            try {
                if (res.succeeded()) {
                    HttpUtils.end204(routingContext, "delete_success");
                } else {
                    Future<Void> resultFuture = Future.future();
                    if (sqlConnection != null)
                        sqlConnection.rollback(resultFuture.completer());
                    logger.error("deleteResultHandlerClose failed!", res.cause());
                    HttpUtils.internalError(routingContext, res.cause());

                }
            } finally {
                if (sqlConnection != null)
                    sqlConnection.close();

            }
        };
    }

    public static <T> T handleExceptions(Supplier<T> r, T defaultValue) {
        try {
            return r.get();
        } catch (RuntimeException ex) {
            logger.error("handleExceptions: ", ex);
            return defaultValue;
        }
    }

}
