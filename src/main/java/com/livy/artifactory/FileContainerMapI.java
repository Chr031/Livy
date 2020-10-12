package com.livy.artifactory;

import io.vertx.core.Promise;

public interface FileContainerMapI {

	Promise<Void> put(ArtifactKey artifactKey, FileContent content);

	Promise<FileContent> get(ArtifactKey artifactKey);

}