USE `library`;

CREATE TABLE `users` (
	`identity` INTEGER NOT NULL AUTO_INCREMENT,
	`login` VARCHAR(255) NOT NULL UNIQUE,
	`password` CHAR(32) NOT NULL,
	/*
	 * 0 - администратор (domain.Role.ADMINISTRATOR)
	 * 1 - архтивариус (domain.Role.REGISTRAR)
	 * 2 - библиотекарь (domain.Role.LIBRARIAN)
	 */
	`role` TINYINT NOT NULL CHECK (`role` IN (0, 1, 2)),
	PRIMARY KEY (`identity`)
) ENGINE=INNODB DEFAULT CHARACTER SET utf8;

CREATE TABLE `readers` (
	`identity` INTEGER NOT NULL AUTO_INCREMENT,
	`surname` VARCHAR(255) NOT NULL,
	`name` VARCHAR(255) NOT NULL,
	`patronymic` VARCHAR(255) NOT NULL,
	`library_card_number` VARCHAR(255) NOT NULL UNIQUE,
	`address` VARCHAR(255) NOT NULL,
	`phone` VARCHAR(255) NOT NULL,
	PRIMARY KEY (`identity`)
) ENGINE=INNODB DEFAULT CHARACTER SET utf8;

CREATE TABLE `authors` (
	`identity` INTEGER NOT NULL AUTO_INCREMENT,
	`surname` VARCHAR(255) NOT NULL,
	`name` VARCHAR(255) NOT NULL,
	`patronymic` VARCHAR(255) NOT NULL,
	`year_of_birth` INTEGER NOT NULL,
	`year_of_death` INTEGER,
	PRIMARY KEY (`identity`)
) ENGINE=INNODB DEFAULT CHARACTER SET utf8;

CREATE TABLE `books` (
	`identity` INTEGER NOT NULL AUTO_INCREMENT,
	`inventory_number` VARCHAR(255) NOT NULL UNIQUE,
	`title` VARCHAR(255) NOT NULL,
	`author_identity` INTEGER,
	`year` INTEGER NOT NULL,
	PRIMARY KEY (`identity`),
	FOREIGN KEY (`author_identity`)
	REFERENCES `authors` (`identity`)
	ON UPDATE CASCADE
	ON DELETE RESTRICT
) ENGINE=INNODB DEFAULT CHARACTER SET utf8;

CREATE TABLE `usages` (
	`identity` INTEGER NOT NULL AUTO_INCREMENT,
	`book_identity` INTEGER NOT NULL,
	`reader_identity` INTEGER NOT NULL,
	`delivery_date` DATE NOT NULL,
	`return_date` DATE,
	`plan_return_date` DATE NOT NULL,
	PRIMARY KEY (`identity`),
	FOREIGN KEY (`book_identity`)
	REFERENCES `books` (`identity`)
	ON UPDATE CASCADE
	ON DELETE RESTRICT,
	FOREIGN KEY (`reader_identity`)
	REFERENCES `readers` (`identity`)
	ON UPDATE CASCADE
	ON DELETE RESTRICT
) ENGINE=INNODB DEFAULT CHARACTER SET utf8;