package es.upm.fi.oeg.integration.adapter.pachube.model;

import java.util.Collection;

import com.google.common.collect.Lists;


public class Environment {

		private String id;
		private String timeAlias;
		private String status;
		private String email;
		private String feed;
		private Location location;
		private Collection<Datastream> datastreams;
		
		public Environment()
		{
			datastreams = Lists.newArrayList();
		}
		
		public void setStatus(String status) {
			this.status = status;
		}
		public String getStatus() {
			return status;
		}
		public void setEmail(String email) {
			this.email = email;
		}
		public String getEmail() {
			return email;
		}
		public void setFeed(String feed) {
			this.feed = feed;
		}
		public String getFeed() {
			return feed;
		}
		public void setLocation(Location location) {
			this.location = location;
		}
		public Location getLocation() {
			return location;
		}
		public void setDatastreams(Collection<Datastream> datastreams) {
			this.datastreams = datastreams;
		}
		public Collection<Datastream> getDatastreams() {
			return datastreams;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getId() {
			return id;
		}

		public void setTimeAlias(String timeAlias) {
			this.timeAlias = timeAlias;
		}

		public String getTimeAlias() {
			return timeAlias;
		}
		
	
}
