----1----
ALTER TABLE employees
ADD (salary FLOAT, date_hired DATE, date_fired DATE);

----2----
UPDATE employees
SET salary = 1000, 
    date_hired = '2025-01-24', 
    date_fired = '2025-03-24'
WHERE officeCode BETWEEN 1 AND 7;

----3----
DELIMITER $$

CREATE TRIGGER archive_fired_employee
AFTER DELETE ON employees
FOR EACH ROW

BEGIN
    
    IF OLD.date_fired <= CURDATE() THEN
        INSERT INTO employee_archive 
            (employeeNumber, lastName, firstName, extension, email, officeCode, reportsTo, jobTitle, salary, date_hired, date_fired)
        VALUES 
            (OLD.employeeNumber, OLD.lastName, OLD.firstName, OLD.extension, OLD.email, OLD.officeCode, OLD.reportsTo, OLD.jobTitle, OLD.salary, OLD.date_hired, OLD.date_fired);
    END IF;

END$$

DELIMITER ;

----4----
DELIMITER $$

CREATE FUNCTION check_fired_status(date_fired DATETIME)
RETURNS VARCHAR(10)
DETERMINISTIC

BEGIN
    IF date_fired IS NULL THEN
            RETURN '';
        ELSE
            RETURN 'FIRED';
        END IF;
END $$

DELIMITER ;

----5----
CREATE VIEW EmployeeStatusView AS
SELECT 
    e.employeeNumber,
    e.firstName,
    e.lastName,
    e.salary,
    e.date_hired,
    e.date_fired,
    check_fired_status(e.date_fired) AS status
FROM employees e;

----6----
DELIMITER $$

CREATE PROCEDURE UpdateSalaries()

BEGIN
    -- Update the salary of all employees (excluding those with 'FIRED' status)
    UPDATE employees 
    SET salary = salary * 1.05 
    WHERE check_fired_status(date_fired) != 'FIRED';
    
    -- Log the salary change in the employees table
    INSERT INTO employees (employeeNumber, oldSalary, newSalary, changeDate)
    SELECT 
        employeeNumber, 
        salary / 1.05 AS oldSalary, 
        salary * 1.05 AS newSalary, 
        NOW() AS changeDate
    FROM employees
    WHERE check_fired_status(date_fired) != 'FIRED';

END $$

DELIMITER ;

----7----
DELIMITER $$

CREATE PROCEDURE RemoveFiredEmployees()
BEGIN
    DELETE FROM employees
    WHERE check_fired_status(date_fired) = 'FIRED';
END $$

DELIMITER ;

----8----
CREATE TABLE salaryArchive (
    archiveID INT AUTO_INCREMENT PRIMARY KEY,
    employeeNumber INT,
    oldSalary FLOAT,
    newSalary FLOAT,
    changeDate DATETIME,
    FOREIGN KEY (employeeNumber) REFERENCES employees(employeeNumber)
);




