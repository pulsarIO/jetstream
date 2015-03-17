package com.ebay.jetstream.event.processor.hdfs;

public class DefaultFileNameResolver implements FileNameResolver {
	public static final String SUFFIX_TMP_FILE = ".tmp";

	// injected
	private String fileNamePrefix = "";
	private String fileNameSuffix = "";

	public void setFileNamePrefix(String fileNamePrefix) {
		this.fileNamePrefix = fileNamePrefix;
	}

	public void setFileNameSuffix(String fileNameSuffix) {
		this.fileNameSuffix = fileNameSuffix;
	}

	@Override
	public String getTmpFileName(String topic, int partition, long startOffset) {
		StringBuilder sb = new StringBuilder();
		if (fileNamePrefix != null && !fileNamePrefix.isEmpty()) {
			sb.append(fileNamePrefix).append("-");
		}
		return sb.append(topic).append("-").append(partition).append("-")
				.append(startOffset).append(SUFFIX_TMP_FILE).toString();
	}

	@Override
	public String getDestFileName(String topic, int partition,
			long startOffset, long endOffset) {
		StringBuilder sb = new StringBuilder();
		if (fileNamePrefix != null && !fileNamePrefix.isEmpty()) {
			sb.append(fileNamePrefix).append("-");
		}
		return sb.append(topic).append("-").append(partition).append("-")
				.append(startOffset).append("-").append(endOffset)
				.append(fileNameSuffix).toString();
	}

}
