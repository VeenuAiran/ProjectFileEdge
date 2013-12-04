package poke.resources;

public class DocAddBean {
	Long fileId;
	String fileName;
	String filePath; 
	String namespace;
	boolean isowner;
	public Long getFileid() {
		return fileId;
	}
	public void setFileid(Long fileId) {
		this.fileId = fileId;
	}
	public String getFilename() {
		return fileName;
	}
	public void setFilename(String fileName) {
		this.fileName = fileName;
	}
	public String getFilepath() {
		return filePath;
	}
	public void setFilepath(String filePath) {
		this.filePath = filePath;
	}
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public boolean isIsowner() {
		return isowner;
	}
	public void setIsowner(boolean isowner) {
		this.isowner = isowner;
	}
	@Override
	public String toString() {
		return "DocAddBean [fileId=" + fileId + ", fileName=" + fileName
				+ ", filePath=" + filePath + ", namespace=" + namespace
				+ ", isowner=" + isowner + "]";
	}
	public DocAddBean(Long fileId, String fileName, String filePath,
			String namespace, boolean isowner) {
		super();
		this.fileId = fileId;
		this.fileName = fileName;
		this.filePath = filePath;
		this.namespace = namespace;
		this.isowner = isowner;
	}

}
