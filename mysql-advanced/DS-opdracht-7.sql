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


