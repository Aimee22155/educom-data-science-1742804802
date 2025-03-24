----1----
ALTER TABLE employees
ADD (salary FLOAT, date_hired DATE, date_fired DATE);

----2----
UPDATE employees
SET salary = '1000'
WHERE officeCode BETWEEN 1 AND 7;

----3----
