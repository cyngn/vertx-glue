/*
 * Copyright 2016 Cyanogen Inc. All Rights Reserved.
 */
package com.cyngn.vertx;

import com.cyngn.kafka.consume.SimpleConsumer;
import com.cyngn.kafka.produce.MessageProducer;
import com.cyngn.vertx.async.promise.Promise;
import com.cyngn.vertx.async.promise.PromiseAction;
import com.cyngn.vertx.opentsdb.OpenTsDbOptions;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.englishtown.vertx.cassandra.CassandraSession;
import com.englishtown.vertx.cassandra.impl.DefaultCassandraSession;
import com.englishtown.vertx.cassandra.impl.JsonCassandraConfigurator;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Custom launcher for launching vert.x apps and supporting libraries
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 2/24/16
 */
public class CyngnLauncher extends Launcher {

    private static final Logger logger = LoggerFactory.getLogger(CyngnLauncher.class);
    private JsonObject config;

    public static void main(String[] args) { new CyngnLauncher().dispatch(args); }

    @Override
    public void afterConfigParsed(JsonObject config) {
        this.config = config;
        super.afterConfigParsed(config);
    }

    @Override
    public void beforeStartingVertx(VertxOptions options) {
        // turn on SPI if the config is there
        if(config.containsKey(Constants.OPEN_TS_DB_KEY)) {
            options.setMetricsOptions(
                    new OpenTsDbOptions(config.getJsonObject(Constants.OPEN_TS_DB_KEY))
                            .setEnabled(true));
        }

        super.beforeStartingVertx(options);
    }

    @Override
    public void afterStartingVertx(Vertx vertx) {
        super.afterStartingVertx(vertx);
        Promise.newInstance(vertx).all(
            (context, onResult) -> handleConfig(vertx, onResult),
            (context, onResult) -> handleCassandra(vertx, config, onResult),
            (context, onResult) -> handleKafka(vertx, config, onResult)
        ).done(context -> vertx.eventBus().publish(Constants.LAUNCH_MESSAGE, new JsonObject()))
        .except(context -> vertx.close())
        .timeout(10000) // 10 seconds
        .eval();
    }

    /**
     * Handles initializing Cassandra and shoving it in shared data
     *
     * @param vertx the vertx instance to initialize cassandra with
     * @param config the config holding the cassandra config
     * @param onComplete did the step succeed or fail
     */
    protected void handleCassandra(Vertx vertx, JsonObject config, Consumer<Boolean> onComplete) {
        JsonObject cassandraConfig = config.getJsonObject(Constants.CASSANDRA_KEY);
        if (cassandraConfig != null) {
            CassandraSession session = new DefaultCassandraSession(
                    Cluster.builder().withProtocolVersion(ProtocolVersion.V3),
                    new JsonCassandraConfigurator(cassandraConfig),
                    vertx);
            SharedObject holder = new SharedObject(session);
            session.onReady(result -> {
                if(result.failed()) {
                    logger.error("Failed to startup cassandra", result.cause());
                    onComplete.accept(false);
                }
                LocalMap<String, SharedObject> cSession = vertx.sharedData().getLocalMap(Constants.CASSANDRA_KEY);
                cSession.putIfAbsent(Constants.CASSANDRA_KEY, holder);
                onComplete.accept(true);
            });
        }
    }

    /**
     * Handles supporting deploying the consumer and producer kafka verticles
     *
     * @param vertx the vertx instance to use to deploy the kafka verticle instances
     * @param config the config to check for kafka options
     * @param onComplete did the step succeed or fail
     */
    protected void handleKafka(Vertx vertx, JsonObject config, Consumer<Boolean> onComplete) {
        JsonObject kafkaConsumerConfig = config.getJsonObject(Constants.KAFKA_CONSUMER_KEY);
        JsonObject kafkaProducerConfig = config.getJsonObject(Constants.KAFKA_PRODUCER_KEY);

        if(kafkaConsumerConfig == null && kafkaProducerConfig == null) {
            onComplete.accept(true);
            return;
        }

        // need to wrap the potential multi steps and just signal on the total result
        Promise kafkaInit = Promise.newInstance(vertx);
        List<PromiseAction> actions = new ArrayList<>();
        if(kafkaConsumerConfig != null) {
            actions.add((context, onConsumerDeployed) ->
                    deployVerticle(vertx, SimpleConsumer.class.getName(), kafkaConsumerConfig, onConsumerDeployed));
        }

        if(kafkaProducerConfig != null) {
            actions.add((context, onProducerDeployed) ->
                    deployVerticle(vertx, MessageProducer.class.getName(), kafkaProducerConfig, onProducerDeployed));
        }

        kafkaInit.all(actions.toArray(new PromiseAction[actions.size()]))
                .except(context -> onComplete.accept(false))
                .done(context -> onComplete.accept(true))
                .eval();
    }

    /**
     * Handle shoving the config into shared data, if you need to parse it into a concrete impl just extend and override
     *  this one method.
     *
     * @param vertx the vertx instance
     * @param onComplete did the step succeed or fail
     */
    final protected void handleConfig(Vertx vertx, Consumer<Boolean> onComplete) {
        LocalMap<String, SharedObject> configMap = vertx.sharedData().getLocalMap(Constants.SHARED_CONFIG_KEY);
        configMap.putIfAbsent(Constants.SHARED_CONFIG_KEY, getConfig());
        onComplete.accept(true);
    }

    protected SharedObject getConfig() {
        return new SharedObject(config);
    }

    /**
     * Handles deploying verticles should be no need to ever override this
     *
     * @param vertx the vertx instance to deploy the verticle with
     * @param name the name of the verticle
     * @param verticleConfig the config for a verticle instance
     * @param onComplete did the step succeed or fail
     */
    final protected void deployVerticle(Vertx vertx, String name, JsonObject verticleConfig, Consumer<Boolean> onComplete) {
        vertx.deployVerticle(name, (new DeploymentOptions()).setConfig(verticleConfig), result -> {
            if(result.failed()) {
                logger.error("Failed to deploy verticle: {}", name, result.cause());
                onComplete.accept(false);
            } else {
                logger.info("Successfully deployed verticle: {}", name);
                onComplete.accept(true);
            }
        });
    }

    @Override
    public void beforeDeployingVerticle(DeploymentOptions deploymentOptions) {
        super.beforeDeployingVerticle(deploymentOptions);
    }

    @Override
    public void handleDeployFailed(Vertx vertx, String mainVerticle, DeploymentOptions deploymentOptions, Throwable cause) {
        super.handleDeployFailed(vertx, mainVerticle, deploymentOptions, cause);
    }
}

