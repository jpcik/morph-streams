package es.upm.fi.oeg.integration.adapter.pachube.model;

import java.util.Collection;

import com.google.common.collect.Lists;


public class Dataset 
{
	private int totalResults;
	private int startIndex;
	private int itemsPerPage;
	private Collection<Environment> results;
	
	public Dataset()
	{
		results = Lists.newArrayList();
	}
	
	public void setTotalResults(int totalResults) {
		this.totalResults = totalResults;
	}
	public int getTotalResults() {
		return totalResults;
	}
	public void setStartIndex(int startIndex) {
		this.startIndex = startIndex;
	}
	public int getStartIndex() {
		return startIndex;
	}
	public void setItemsPerPage(int itemsPerPage) {
		this.itemsPerPage = itemsPerPage;
	}
	public int getItemsPerPage() {
		return itemsPerPage;
	}
	public void setResults(Collection<Environment> results) {
		this.results = results;
	}
	public Collection<Environment> getResults() {
		return results;
	}
}
