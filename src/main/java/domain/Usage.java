package domain;

import java.util.Date;

public class Usage extends Entity {
	private Book book;
	private Reader reader;
	private Date deliveryDate;
	private Date returnDate;
	private Date planReturnDate;

	public Book getBook() {
		return book;
	}

	public void setBook(Book book) {
		this.book = book;
	}

	public Reader getReader() {
		return reader;
	}

	public void setReader(Reader reader) {
		this.reader = reader;
	}

	public Date getDeliveryDate() {
		return deliveryDate;
	}

	public void setDeliveryDate(Date deliveryDate) {
		this.deliveryDate = deliveryDate;
	}

	public Date getReturnDate() {
		return returnDate;
	}

	public void setReturnDate(Date returnDate) {
		this.returnDate = returnDate;
	}

	public Date getPlanReturnDate() {
		return planReturnDate;
	}

	public void setPlanReturnDate(Date planReturnDate) {
		this.planReturnDate = planReturnDate;
	}
}
