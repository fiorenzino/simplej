package nz.fiore.vertx.test;

import nz.fiore.simplej.model.Whisky;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Created by fiorenzo on 16/04/17.
 */
public class JsonTest {

    @Test
    public void json() {
        Whisky whisky = new Whisky(UUID.randomUUID().toString(), "flower");
        whisky.date = Instant.now();
        whisky.amount = new BigDecimal(33.5);
        System.out.println(whisky.toJson().toString());
    }

    @Test
    public void jsonWithNull() {
        Whisky whisky = new Whisky(UUID.randomUUID().toString(), "flower");
        whisky.date = null;
        whisky.amount = null;
        System.out.println(whisky.toJson().toString());
    }

}
