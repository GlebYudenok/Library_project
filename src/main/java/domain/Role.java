package domain;

public enum Role {
	ADMINISTRATOR("администратор"),
	REGISTRAR("архивариус"),
	LIBRARIAN("библиотекарь");

	private String name;

	private Role(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public Integer getIdentity() {
		return ordinal();
	}

	public static Role getByIdentity(Integer identity) {
		return Role.values()[identity];
	}
}
