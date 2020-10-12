package com.livy.artifactory;

import java.io.File;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;

public class FlatFileContainer implements FileContainerMapI {

	private final File artifactDirectory;
	
	private final FileSystem fs;

	public FlatFileContainer(Vertx vertx, File artifactDirectory) {
		
		this.fs = vertx.fileSystem();
		this.artifactDirectory = artifactDirectory;
		if (!this.artifactDirectory.exists())
			this.artifactDirectory.mkdirs();
		else if (!this.artifactDirectory.isDirectory())
			throw new RuntimeException(this.artifactDirectory.getAbsolutePath() + " is not a directory");
	}

	@Override
	public Promise<Void> put(ArtifactKey artifactKey, FileContent content) {
		Promise<Void> p = Promise.promise();
		try {
			String directoryPath = artifactKey.buildDirectoryPath(artifactDirectory);
			String filePath = new File(directoryPath, artifactKey.getFileName()).getAbsolutePath();

			fs.mkdirs(directoryPath, (res) -> {
				if (res.succeeded()) {
					fs.writeFile(filePath, Buffer.buffer(content.content), res1 -> {
						if (res1.succeeded()) {
							p.complete();
						} else {
							p.fail(new Exception("Unable to write file ", res1.cause()));
						}
					});
				} else {
					p.fail(new Exception("Unable to create the directories", res.cause()));
				}
			});
		} catch (Exception e) {
			p.fail(new Exception("Fatal, cannot create the new repository entry ", e));
		}
		return p;

	}

	@Override
	public Promise<FileContent> get(ArtifactKey artifactKey) {
		Promise<FileContent> p = Promise.promise();
		try {
			String directoryPath = artifactKey.buildDirectoryPath(artifactDirectory);
			String filePath = new File(directoryPath, artifactKey.getFileName()).getAbsolutePath();
			fs.readFile(filePath, res -> {
				if (res.succeeded()) {
					FileContent fileContent = new FileContent(artifactKey, res.result().getBytes(), "application/octet-stream");
					p.complete(fileContent);
				} else {
					p.fail(new Exception("Unable to read file " + artifactKey , res.cause()));
				}
			});
		} catch (Exception e) {
			p.fail(new Exception("Fatal, cannot read the repository entry ", e));
		}
		return p;
	}

}