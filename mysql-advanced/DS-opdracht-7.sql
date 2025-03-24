----1----
ALTER TABLE employees
ADD (salary FLOAT, date_hired DATE, date_fired DATE);

----2----
INSERT INTO employees (salary, date_hired, date_fired)
VALUES ('2500', '2025-01-24', '2025-03-24');

----3----
