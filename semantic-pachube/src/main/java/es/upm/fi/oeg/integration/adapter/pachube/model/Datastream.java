package es.upm.fi.oeg.integration.adapter.pachube.model;

public class Datastream 
{
	private String id;
	private String alias;
	private String timeAlias; 
	private String[] tags;
	private String at;
	private String current_value;
	public void setTags(String[] tags) {
		this.tags = tags;
	}
	public String[] getTags() {
		return tags;
	}
	public void setAt(String at) {
		this.at = at;
	}
	public String getAt() {
		return at;
	}
	public void setCurrent_value(String current_value) {
		this.current_value = current_value;
	}
	public String getCurrent_value() {
		return current_value;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public String getAlias() {
		return alias;
	}
	public void setTimeAlias(String timeAlias) {
		this.timeAlias = timeAlias;
	}
	public String getTimeAlias() {
		return timeAlias;
	}
}
