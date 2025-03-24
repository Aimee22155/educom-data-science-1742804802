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
