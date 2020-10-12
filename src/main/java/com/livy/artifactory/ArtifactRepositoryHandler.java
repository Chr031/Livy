package com.livy.artifactory;

import java.io.File;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

public class ArtifactRepositoryHandler implements Handler<RoutingContext> {

	private final static Logger log = LogManager.getLogger(ArtifactRepositoryHandler.class);

	private final String repositoryName;

	private final FileContainerMapI fileContainerMap;

	private final Vertx vertx;

	public ArtifactRepositoryHandler(Vertx vertx, String repositoryName, FileContainerMapI FileContainer) {
		this.repositoryName = repositoryName;
		this.vertx = vertx;
		fileContainerMap = FileContainer;

		log.info("Artifactory '" + repositoryName + "' started.");
	}

	public ArtifactRepositoryHandler(Vertx vertx, String repositoryName, File artifactDirectory) {
		this(vertx, repositoryName, new FlatFileContainer(vertx, artifactDirectory));

	}

	@Override
	public void handle(RoutingContext context) {

		log.debug("Factory received request : " + context.request().method() + " " + context.request().absoluteURI());
		/**
		 * log.info("Headers " + context.request().headers().entries().stream().map(e ->
		 * e.getKey() + " : " + e.getValue()) .collect(Collectors.joining(" | ")));
		 **/
		switch (context.request().method()) {
		case PUT:
			saveArtifact(context).future()
					.onSuccess(res -> context.response().end("ok"))
					.onFailure(error -> {
						context.response().setStatusCode(500).end();
						log.error("Data not saved : " + error.getMessage(), error);
					});
			break;
		case GET:
			readArtifact(context).future().onComplete(res -> {
				if (res.succeeded()) {
					FileContent content = res.result();
					context.response()
							.putHeader("Content-Type", content.getContentType())
							.putHeader("Content-Length", "" + content.getContent().length)
							.end(Buffer.buffer(content.getContent()));

				} else {
					context.response().setStatusCode(404).end();
					log.warn("Data not found", res.cause());
				}
			});
			break;
		case HEAD:
			readArtifact(context).future().onComplete(res -> {
				if (res.succeeded()) {
					FileContent content = res.result();
					context.response()
							.putHeader("Content-Type", content.getContentType())
							.putHeader("Content-Length", "" + content.getContent().length)
							.end();

				} else {
					context.response().setStatusCode(404).end();
					log.warn("Data not found", res.cause());
				}
			});
			break;

		default:
			log.info("Method not implemented : " + context.request().method());
			context.response().setStatusCode(400).end();
		}

	}

	private Promise<FileContent> readArtifact(RoutingContext context) {
		Promise<FileContent> p = Promise.promise();
		vertx.runOnContext((res) -> {
			ArtifactKey artifactKey = getArtifactKey(context);
			fileContainerMap.get(artifactKey).future()
					.onSuccess(content -> {
						p.complete(content);
						log.info("Data found : " + artifactKey);
					})
					.onFailure(error -> {
						p.fail("No data found");
						log.warn("Artifact " + artifactKey + " not found");
					});

		});

		return p;
	}

	private Promise<ArtifactKey> saveArtifact(RoutingContext routingContext) {
		Promise<ArtifactKey> f = Promise.promise();
		vertx.runOnContext((res) -> {

			try {
				ArtifactKey artifactKey = getArtifactKey(routingContext);
				String contentType = routingContext.request().getHeader("Content-Type");
				byte[] bytes = routingContext.getBody().getBytes();
				FileContent content = new FileContent(artifactKey, bytes, contentType);

				fileContainerMap.put(artifactKey, content).future()
						.onSuccess(v -> {
							f.complete(artifactKey);
							log.info("Artifact " + artifactKey + " saved");
						})
						.onFailure(error -> {
							log.error("Unable to save artifact " + artifactKey + " in " + repositoryName, error);
							f.fail(error);
						});

			} catch (Exception e) {
				log.fatal("Unable to save artifact from " + routingContext.request().path() + repositoryName, e);
				f.fail(e);

			}
		});
		return f;
	}

	private ArtifactKey getArtifactKey(RoutingContext context) {
		String group = context.pathParam("group");
		String artifactName = context.pathParam("name");
		String version = context.pathParam("version");
		String fileName = context.pathParam("fileName");

		return new ArtifactKey(group, artifactName, version, fileName);
	}
}
