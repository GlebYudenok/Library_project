package domain;

import java.util.ArrayList;
import java.util.List;

public class Reader extends Person {
	private String libraryCardNumber;
	private String address;
	private String phone;
	private List<Usage> returnedUsages = new ArrayList<>();
	private List<Usage> currentUsages = new ArrayList<>();
	private List<Usage> overdueUsages = new ArrayList<>();

	public String getLibraryCardNumber() {
		return libraryCardNumber;
	}

	public void setLibraryCardNumber(String libraryCardNumber) {
		this.libraryCardNumber = libraryCardNumber;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public List<Usage> getReturnedUsages() {
		return returnedUsages;
	}

	public List<Usage> getCurrentUsages() {
		return currentUsages;
	}

	public List<Usage> getOverdueUsages() {
		return overdueUsages;
	}
}
