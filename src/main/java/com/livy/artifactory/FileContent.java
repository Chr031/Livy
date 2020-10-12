package com.livy.artifactory;

import java.io.Serializable;

public class FileContent implements Serializable {

	private static final long serialVersionUID = -3277979135154590364L;

	private final ArtifactKey key;

	final byte[] content;

	private final String contentType;

	public FileContent(ArtifactKey key, byte[] content, String contentType) {
		super();
		this.key = key;
		this.content = content;
		this.contentType = contentType;
	}

	public String getFileName() {
		return key.getFileName();
	}

	public byte[] getContent() {
		return content;
	}

	public String getContentType() {
		return contentType;
	}

}