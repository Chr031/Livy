package com.livy.artifactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.MimeMapping;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * 
 * We use a different sort of caching from most other web servers - the sha1
 * hash of the file contents is used as an ETAG. Most other servers will use a
 * hash of the last modified time and file size. This is a problem in a multi
 * server environment as files on different servers may have different modified
 * dates based on server deployment times, etc. By using the sha1 we ensure that
 * files with the same ETAG will be cached across all servers.
 * 
 * Calculating the SHA1 is expensive, so we do it in-line with transmitting the
 * file, and we cache the result and only re-check it when the file modification
 * date we have cached changes.
 * 
 */
public class StaticFileHandler {

	private static final Logger log = Logger.getLogger(StaticFileHandler.class);

	private static final String DEFAULT_FILE = "index.html";

	final Path staticPath;
	final String staticPathStr;
	final Vertx vertx;

	private static final Map<String, FileCacheInfo> cacheMap = new ConcurrentHashMap<>();

	public StaticFileHandler(Vertx vertx, String staticPathStr) {
		this.vertx = vertx;
		this.staticPathStr = staticPathStr;

		staticPath = FileSystems.getDefault().getPath(staticPathStr).normalize();
	}

	public void handle(final HttpServerRequest request) {
		String pathDecoded;
		try {
			pathDecoded = URLDecoder.decode(request.path(), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			sendNotFound(request);
			return;
		}

		log.info("[" + request.remoteAddress().host() + "] " + request.absoluteURI().toString() + " || " + request.method().name() + " " + pathDecoded);

		if (!"GET".equals(request.method().name())) {
			sendNotFound(request);
			return;
		}

		Path requestPath = FileSystems.getDefault().getPath(staticPathStr, pathDecoded).normalize();

		// Ensure path request is inside statics path
		if (!requestPath.startsWith(staticPath)) {
			log.info("Attempt to access outside of path");
			sendNotFound(request);
			return;
		}

		handleRequestString(request, requestPath.toString());

	}

	private void handleRequestString(final HttpServerRequest request, final String requestStr) {
		final FileSystem fileSystem = vertx.fileSystem();
		fileSystem.exists(requestStr, new Handler<AsyncResult<Boolean>>() {

			@Override
			public void handle(AsyncResult<Boolean> exists) {
				if (!exists.result()) {
					sendNotFound(request);
					return;
				}

				fileSystem.props(requestStr, new Handler<AsyncResult<FileProps>>() {

					@Override
					public void handle(AsyncResult<FileProps> event) {
						FileProps props = event.result();
						testFileAndSend(request, requestStr, props);
					}
				});
			}
		});
	}

	private void testFileAndSend(final HttpServerRequest request, final String requestStr, FileProps props) {
		if (props.isDirectory()) {
			vertx.fileSystem().exists(requestStr + File.separator + DEFAULT_FILE, new Handler<AsyncResult<Boolean>>() {

				@Override
				public void handle(AsyncResult<Boolean> exists) {
					String absUri = request.absoluteURI();
					absUri += absUri.endsWith("/") ? "" : "/";
					absUri += exists.result() ? DEFAULT_FILE : "";

					log.debug("Redirect : " + absUri);
					if (!exists.result() && absUri.endsWith("/")) {
						sendFileListing(request, requestStr);
					} else {
						sendRedirect(request, absUri);
					}

				}
			});

			return;

		}
		if (!props.isRegularFile()) {
			sendNotFound(request);
			return;
		}

		// file will be sent : add the location
		// request.response().putHeader("Location",
		// "http://localhost:2016/codemirror-5.19.0/" );

		FileCacheInfo cacheInfo = cacheMap.get(requestStr);
		String etag = request.headers().get("If-None-Match");

		if (cacheInfo != null && cacheInfo.lastModifiedTime == props.lastModifiedTime()) {
			// Last modified time has not changed for this file, we don't need
			// to recalculate the sha1 of the contents
			if (etag != null && etag.equals(cacheInfo.etagsha1)) {
				sendNotChanged(request);
			} else {
				sendFile(request, requestStr, cacheInfo);
			}
		} else {
			// Last modified time has changed - we need to send the file and
			// also calculate sha1 of the contents
			sendFileAndCache(request, requestStr, props);
		}
	}

	protected void sendFileListing(HttpServerRequest request, String requestStr) {

		vertx.fileSystem().readDir(requestStr, new Handler<AsyncResult<List<String>>>() {

			@Override
			public void handle(AsyncResult<List<String>> fileList) {

				if (fileList.succeeded()) {
					fileList.result();
					log.info(Arrays.toString(fileList.result().toArray()));
					try {
						request.response().putHeader("Content-Location", request.absoluteURI() + "/");
						sendModel(request, new IndexModel(URLDecoder.decode(request.path(), "UTF-8"), fileList.result()));
					} catch (UnsupportedEncodingException e) {
						sendNotFound(request);
					}
				} else {
					sendNotFound(request);
				}
			}
		});
	}

	protected void sendModel(HttpServerRequest request, Model model) {
		model.processHttpRequest(request);
	}

	private void sendFileAndCache(final HttpServerRequest request, final String requestStr, final FileProps props) {
		request.response().putHeader("Content-Length", Long.toString(props.size()));
		int li = requestStr.lastIndexOf('.');
		if (li != -1 && li != requestStr.length() - 1) {
			String ext = requestStr.substring(li + 1, requestStr.length());
			String contentType = MimeMapping.getMimeTypeForExtension(ext);
			if (contentType != null) {
				request.response().putHeader("Content-Type", contentType);
			}
		}
		OpenOptions oo = new OpenOptions();
		// null, true, false, false,
		oo.setPerms(null).setCreate(true).setCreateNew(false).setSync(false);
		vertx.fileSystem().open(requestStr, oo, new Handler<AsyncResult<AsyncFile>>() {

			@Override
			public void handle(AsyncResult<AsyncFile> event) {
				final AsyncFile asyncFile = event.result();
				if (asyncFile == null) {
					request.response().end();
					return;
				}

				final Sha1PumpToHttp pump = new Sha1PumpToHttp(asyncFile, request.response());
				asyncFile.endHandler(new Handler<Void>() {

					@Override
					public void handle(Void event) {
						FileCacheInfo fileCacheInfo = new FileCacheInfo(props.lastModifiedTime(), pump.getSHA1Hash());
						cacheMap.put(requestStr, fileCacheInfo);
						asyncFile.close();

						// Unfortunately we can't send the new ETAG to this
						// request as the ETAG must be sent in the header, but
						// the next request will get it.
						request.response().end();
					}
				});

				pump.start();
			}
		});
	}

	private void sendFile(HttpServerRequest request, String requestStr, FileCacheInfo cacheInfo) {
		request.response().putHeader("ETag", cacheInfo.etagsha1);
		request.response().sendFile(requestStr);
	}

	private void sendNotFound(HttpServerRequest request) {
		log.warn("not found : " + request.absoluteURI());
		request.response().setStatusCode(404).end("Not found");
	}

	private void sendNotChanged(HttpServerRequest request) {
		request.response().setStatusCode(304).end();
	}

	private void sendRedirect(HttpServerRequest request, String redirectAddress) {

		request.response().putHeader("Location", redirectAddress);
		request.response().setStatusCode(301).end("Moved Permanently");
	}

	private static class FileCacheInfo {

		final long lastModifiedTime;
		final String etagsha1;

		public FileCacheInfo(long lastModifiedTime, String etagsha1) {
			this.lastModifiedTime = lastModifiedTime;
			this.etagsha1 = etagsha1;
		}

	}

	/**
	 * A copy of Pump that also creates an SHA1 hash of the stream as it passes
	 * through. Can be fetched once all data has been pushed through with
	 * getSHA1Hash()
	 */
	public class Sha1PumpToHttp {

		private final ReadStream<Buffer> readStream;
		private final WriteStream<Buffer> writeStream;
		private int pumped;
		private MessageDigest md;

		/**
		 * Start the Pump. The Pump can be started and stopped multiple times.
		 */
		public Sha1PumpToHttp start() {
			readStream.handler(dataHandler);
			return this;
		}

		/**
		 * Stop the Pump. The Pump can be started and stopped multiple times.
		 */
		public Sha1PumpToHttp stop() {
			writeStream.drainHandler(null);
			readStream.handler(null);
			return this;
		}

		/**
		 * Return the total number of bytes pumped by this pump.
		 */
		public int bytesPumped() {
			return pumped;
		}

		/**
		 * Return a hex string of the sha1 hash of the data that passed through
		 */
		public String getSHA1Hash() {
			return convertToHex(md.digest());
		}

		private String convertToHex(byte[] data) {
			// Create Hex String
			StringBuffer hexString = new StringBuffer();
			for (int i = 0; i < data.length; i++) {
				String h = Integer.toHexString(0xFF & data[i]);
				while (h.length() < 2)
					h = "0" + h;
				hexString.append(h);
			}
			return hexString.toString();
		}

		private final Handler<Void> drainHandler = new Handler<Void>() {
			@Override
			public void handle(Void v) {
				readStream.resume();
			}
		};

		private final Handler<Buffer> dataHandler = new Handler<Buffer>() {
			@Override
			public void handle(Buffer buffer) {
				md.update(buffer.getBytes());
				writeStream.write(buffer);
				pumped += buffer.length();
				if (writeStream.writeQueueFull()) {
					readStream.pause();
					writeStream.drainHandler(drainHandler);
				}
			}
		};

		public Sha1PumpToHttp(ReadStream<Buffer> rs, WriteStream<Buffer> ws) {
			readStream = rs;
			writeStream = ws;
			try {
				md = MessageDigest.getInstance("SHA-1");
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
		}

	}

	public class Model {

		private final Map<String, String> arguments;

		private final CompletableFuture<String> templateContent;

		public Model(String templateFile) {

			arguments = new HashMap<String, String>();
			templateContent = new CompletableFuture<>();
			vertx.fileSystem().readFile(templateFile, result -> {
				if (result.succeeded())
					templateContent.complete(result.result().toString());
				else
					templateContent.completeExceptionally(result.cause());
			});
		}

		public Model(int marker, String resource) {

			arguments = new HashMap<String, String>();
			templateContent = new CompletableFuture<>();
			InputStream is = getClass().getClassLoader().getResourceAsStream(resource);
			byte b[] = new byte[1024];
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int r = 0;
			try {
				while ((r = is.read(b)) > 0) {
					baos.write(b, 0, r);
				}
				templateContent.complete(baos.toString());
				is.close();
			} catch (Exception e) {
				templateContent.completeExceptionally(e);
			}
		}

		public long fileSize() {
			return 0;
		}

		public void processHttpRequest(HttpServerRequest request) {
			vertx.executeBlocking(future -> {
				try {
					String content = templateContent.get();
					for (Entry<String, String> arg : arguments.entrySet()) {
						content = content.replace("#{" + arg.getKey() + "}", arg.getValue());
					}
					future.complete(content);

				} catch (Exception e) {
					log.error("Unable to process model ", e);

					future.fail(e);
				}

			}, (Handler<AsyncResult<String>>) res -> {
				if (res.succeeded()) {
					request.response()
							.putHeader("Content-Length", Long.toString(res.result().length()))
							.putHeader("Content-Type", MimeMapping.getMimeTypeForExtension("html"))

							.write(res.result()).end();
				} else {
					sendNotFound(request);
				}
			});

		}

		public String getContentType() {
			// TODO Auto-generated method stub
			return null;
		}

		public long getFileSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		protected void setArgument(String argumentName, String argumentValue) {
			this.arguments.put(argumentName, argumentValue);

		}

		protected String escapeString(String s) {
			return s.replace("\\", "\\\\");
		}

	}

	public class IndexModel extends Model {

		public IndexModel(String requestStr, List<String> fileList) {
			super(1, "baseIndex.html");
			StringBuffer fileListBuff = new StringBuffer("[ ");
			List<File> files = fileList.stream().map(File::new).collect(Collectors.toList());

			// fileList.forEach(f -> fileListBuff.append("\"" +
			// escapeString(s.substring(s.lastIndexOf("\\")+1)) + "\","));
			// files.stream().map(f -> f.getName() + );

			files.forEach(f -> fileListBuff.append("\"" + f.getName() + "\","));
			setArgument("dirList", fileListBuff.substring(0, fileListBuff.length() - 1) + " ]");
			setArgument("currentDir", "\"" + escapeString(requestStr) + "\"");

		}

		public long fileSize() {
			// TODO Auto-generated method stub
			return 0;
		}

	}
}
