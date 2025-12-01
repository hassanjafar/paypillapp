CREATE TABLE `paybilluniquerybill` (
  `id` int NOT NULL AUTO_INCREMENT,
  `accountNumber` varchar(50) DEFAULT NULL,
  `accountTitle` varchar(50) DEFAULT NULL,
  `billAmount` varchar(50) DEFAULT NULL,
  `description` varchar(50) DEFAULT NULL,
  `dueDate` varchar(50) DEFAULT NULL,
  `isPartialPayAllowed` varchar(50) DEFAULT NULL,
  `studentId` varchar(45) DEFAULT NULL,
  `studentName` varchar(45) DEFAULT NULL,
  `currencyCode` varchar(45) DEFAULT 'USD',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=latin1;

CREATE TABLE `multibillpayment` (
  `id` int NOT NULL AUTO_INCREMENT,
  `transactionId` varchar(50) DEFAULT NULL,
  `studentId` varchar(50) DEFAULT NULL,
  `studentName` varchar(50) DEFAULT NULL,
  `totalAmount` varchar(45) DEFAULT NULL,
  `paidAt` varchar(45) DEFAULT NULL,
  `dueDate` varchar(45) DEFAULT NULL,
  `datePaid` varchar(45) DEFAULT NULL,
  `accountNumber` varchar(45) DEFAULT NULL,
  `accountTitle` varchar(45) DEFAULT NULL,
  `desction` varchar(45) DEFAULT NULL,
  `amount` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=206 DEFAULT CHARSET=latin1;


CREATE TABLE `paybilltest` (
  `id` int NOT NULL,
  `invoiceId` varchar(45) DEFAULT NULL,
  `billNumber` varchar(45) DEFAULT NULL,
  `billerName` varchar(50) DEFAULT NULL,
  `billId` varchar(45) DEFAULT NULL,
  `billTo` varchar(45) DEFAULT NULL,
  `billAmount` int DEFAULT NULL,
  `billCurrency` varchar(45) DEFAULT 'USD',
  `dueDate` datetime DEFAULT NULL,
  `partialPayAllowed` int DEFAULT '1',
  `description` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `paynotificationbill` (
  `id` int NOT NULL AUTO_INCREMENT,
  `invoiceId` varchar(45) DEFAULT NULL,
  `paidBy` varchar(45) DEFAULT NULL,
  `paidAt` varchar(45) DEFAULT NULL,
  `paidDate` datetime DEFAULT NULL,
  `billTo` varchar(45) DEFAULT NULL,
  `dueDate` varchar(50) DEFAULT NULL,
  `description` varchar(45) DEFAULT NULL,
  `billerName` varchar(45) DEFAULT NULL,
  `isPrepaid` varchar(45) DEFAULT NULL,
  `tansactionId` varchar(45) DEFAULT NULL,
  `amount` varchar(45) DEFAULT NULL,
  `currency` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=latin1;




DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `QueryMultiBillInfo`(IN p_studentId VARCHAR(255))
BEGIN
    -- First result set: total amount
    SELECT 
        studentId,
        studentName,
        SUM(billAmount) AS totalAmount,
        currencyCode AS currency 
    FROM paybilluniquerybill 
    WHERE studentId = p_studentId AND billAmount > 0
    GROUP BY studentId, studentName, currencyCode;

    -- Second result set: individual bills
    SELECT 
        accountNumber,
        accountTitle,
        billAmount AS amount,
        description,
        dueDate,
        isPartialPayAllowed  
    FROM paybilluniquerybill 
    WHERE studentId = p_studentId AND billAmount > 0;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `PayMultiBill`(
    IN p_transactionId VARCHAR(255),
    IN p_studentId VARCHAR(255),
    IN p_studentName VARCHAR(255),
    IN p_totalAmount DECIMAL(10, 2),
    IN p_paidAt VARCHAR(255),
    IN p_dueDate VARCHAR(255),
    IN p_accountNumber VARCHAR(255),
    IN p_accountTitle VARCHAR(255),
    IN p_description VARCHAR(255),
    IN p_amount DECIMAL(10, 2)
)
BEGIN
    -- Insert into payment log
    INSERT INTO `billingserver`.`multibillpayment`
        (`transactionId`, `studentId`, `studentName`, `totalAmount`, `paidAt`, `dueDate`, `datePaid`, `accountNumber`, `accountTitle`, `desction`, `amount`)
    VALUES
        (p_transactionId, p_studentId, p_studentName, p_totalAmount, p_paidAt, p_dueDate, SYSDATE(), p_accountNumber, p_accountTitle, p_description, p_amount);

    -- Update the outstanding bill
    UPDATE paybilluniquerybill
    SET billAmount = billAmount - p_amount
    WHERE studentId = p_studentId AND accountNumber = p_accountNumber;
END$$
DELIMITER ;


DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `QueryBillInfo`(IN p_invoiceId VARCHAR(255))
BEGIN
    SELECT * FROM paybilltest WHERE billId = p_invoiceId AND billAmount > 0;
END$$
DELIMITER ;



DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `QueryBillInfo`(IN p_invoiceId VARCHAR(255))
BEGIN
    SELECT * FROM paybilltest WHERE billId = p_invoiceId AND billAmount > 0;
END$$
DELIMITER ;
DELIMITER $$
CREATE DEFINER=`root`@`%` PROCEDURE `PayBillNotification`(
    IN p_invoiceId VARCHAR(255),
    IN p_paidBy VARCHAR(255),
    IN p_paidAt VARCHAR(255),
    IN p_paidDate VARCHAR(255),
    IN p_billTo VARCHAR(255),
    IN p_dueDate VARCHAR(255),
    IN p_description VARCHAR(255),
    IN p_billerName VARCHAR(255),
    IN p_isPrepaid BOOLEAN,
    IN p_transactionId VARCHAR(255),
    IN p_amount DECIMAL(10, 2),
    IN p_currency VARCHAR(10)
)
BEGIN
    INSERT INTO `billingserver`.`paynotificationbill`
        (`invoiceId`, `paidBy`, `paidAt`, `paidDate`, `billTo`, `dueDate`, `description`, `billerName`, `isPrepaid`, `tansactionId`, `amount`, `currency`)
    VALUES
        (p_invoiceId, p_paidBy, p_paidAt, p_paidDate, p_billTo, p_dueDate, p_description, p_billerName, p_isPrepaid, p_transactionId, p_amount, p_currency);
END$$
DELIMITER ;




