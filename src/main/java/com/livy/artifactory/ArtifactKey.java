package com.livy.artifactory;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ArtifactKey {

	private final String group;
	private final String artifactName;
	private final String version;
	private final String fileName;

	public ArtifactKey(String group, String artifactName, String version, String fileName) {
		super();
		this.group = group;
		this.artifactName = artifactName;
		this.version = version;
		this.fileName = fileName;
	}

	public String getGroup() {
		return group;
	}

	public String getArtifactName() {
		return artifactName;
	}

	public String getVersion() {
		return version;
	}

	public String getFileName() {
		return fileName;
	}

	public String buildDirectoryPath(File artifactDirectory) {
		return new File(artifactDirectory, Stream.of(group, artifactName, version).collect(Collectors.joining(File.separator))).getAbsolutePath();
	}

}