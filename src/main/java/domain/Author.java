package domain;

public class Author extends Person {
	private Integer yearOfBirth;
	private Integer yearOfDeath;

	public Integer getYearOfBirth() {
		return yearOfBirth;
	}

	public void setYearOfBirth(Integer yearOfBirth) {
		this.yearOfBirth = yearOfBirth;
	}

	public Integer getYearOfDeath() {
		return yearOfDeath;
	}

	public void setYearOfDeath(Integer yearOfDeath) {
		this.yearOfDeath = yearOfDeath;
	}
}
