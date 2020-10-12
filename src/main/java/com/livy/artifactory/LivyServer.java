package com.livy.artifactory;

import java.io.File;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class LivyServer {

	private final static Logger log = LogManager.getLogger(LivyServer.class);

	public static void main(String args[]) {
		Config config = null;

		if (args.length == 1) {
			System.out.println("with 1 args : the xml config file path ... ");
			config = new Config(args[0]);
		} else {
			config = new Config();
		}

		new LivyServer(config).start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("*********************************************");
			log.info("********** Livy shut down properly **********");
			log.info("*********************************************");

		}));

	}

	private final Config config;

	private final Vertx vertx;
	private final HttpServer server;

	public LivyServer(Config config) {
		this.config = config;
		File rootDir = new File(config.defaultPath);

		log.info("*********************************************");
		log.info("********* Livy Server is starting ***********");
		log.info("*********************************************");

		if (!rootDir.exists()) {
			String result = rootDir.mkdirs() ? "Root directory created " : " Root directory not created";
			log.info(result);
		}
		if (!rootDir.isDirectory())
			throw new RuntimeException(rootDir.getAbsolutePath() + " is not a directory");

		log.info("Livy Root Directory : " + rootDir.getAbsolutePath());
		
		vertx = Vertx.vertx(new VertxOptions()
				.setWorkerPoolSize(40)
				.setBlockedThreadCheckInterval(1000 * 60 * 10l));

		/*
		 * <code>NetServerOptions options = new NetServerOptions() .setSsl(true)
		 * .setPemKeyCertOptions( new PemKeyCertOptions() .setKeyPath("./key.pem")
		 * .setCertPath("./cert.pem") ); NetServer server =
		 * vertx.createNetServer(options);</code>
		 */

		HttpServerOptions options = new HttpServerOptions();

		/*
		 * log. info("Secure Transport Protocol [ SSL/TLS ] has been enabled !!! ");
		 * options.setSsl(true).setPemKeyCertOptions(new PemKeyCertOptions()
		 * .setKeyPath("./key.pem") .setCertPath("./cert.pem") );
		 */

		server = vertx.createHttpServer(options);
		String rootPath = rootDir.getAbsolutePath();
		for (File root : File.listRoots()) {
	        if (rootDir.getAbsolutePath().startsWith(root.getAbsolutePath())) {
	        	rootPath = rootDir.getAbsolutePath().substring(root.getAbsolutePath().length());
	        }
		}

		StaticFileHandler fileHandler = new StaticFileHandler(vertx,rootDir.getAbsolutePath());

		Router router = Router.router(vertx);

		router.route("/artifactory/:group/:name/:version/:fileName").handler(BodyHandler.create());
		router.route("/artifactory/:group/:name/:version/:fileName").handler(new ArtifactRepositoryHandler(vertx, "artifactory", rootDir));

		router.route().method(HttpMethod.GET).handler(rCtx -> {
			log.debug("Host : " + rCtx.request().getHeader("host"));
			fileHandler.handle(rCtx.request());
		});

		server.requestHandler(req -> router.accept(req));
	}

	public void start() {

		server.listen(config.port,
				res -> {
					if (res.succeeded()) {
						log.info("Livy server started on port " + config.port);
					} else {
						log.error("Error: Livy server not started : " + res.cause(), res.cause());

						System.exit(1);
					}
				});

	}

	static class Config {

		int port;
		String defaultPath;

		public Config() {
			port = 12020;
			defaultPath = "./artifactory";
		}

		public Config(String fileNameAndPath) {

		}

	}
}
