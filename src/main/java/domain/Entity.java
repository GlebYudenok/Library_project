package domain;

import java.io.Serializable;

abstract public class Entity implements Serializable {
	private Integer identity;

	public Integer getIdentity() {
		return identity;
	}

	public void setIdentity(Integer identity) {
		this.identity = identity;
	}

	@Override
	public boolean equals(Object object) {
		if(object != null) {
			if(object != this) {
				if(object.getClass() == getClass() && identity != null) {
					return identity.equals(((Entity)object).identity);
				}
				return false;
			}
			return true;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return identity != null ? identity.hashCode() : 0;
	}
}
