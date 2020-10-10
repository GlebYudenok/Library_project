package domain;

import java.util.ArrayList;
import java.util.List;

public class Book extends Entity {
	private String inventoryNumber;
	private String title;
	private Author author;
	private Integer year;
	private Usage currentUsage;
	private List<Usage> usages = new ArrayList<>();

	public String getInventoryNumber() {
		return inventoryNumber;
	}

	public void setInventoryNumber(String inventoryNumber) {
		this.inventoryNumber = inventoryNumber;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Author getAuthor() {
		return author;
	}

	public void setAuthor(Author author) {
		this.author = author;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Usage getCurrentUsage() {
		return currentUsage;
	}

	public void setCurrentUsage(Usage currentUsage) {
		this.currentUsage = currentUsage;
	}

	public List<Usage> getUsages() {
		return usages;
	}
}
