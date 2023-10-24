--How many unique values are in User Id?

SELECT 
    Age,
    Sex,
    Job,
    Nationality,
    AVG(IF(target_income = '>50K', 1, 0)) AS average_income
FROM bank_income
GROUP BY Age, Sex, Job, Nationality;

---What is the average of Target Income grouped by Age, Sex, Job, and Nationality?

SELECT 
    Age,
    Sex,
    Job,
    Nationality,
    AVG(IF(target_income = '>50K', 1, 0)) AS average_income
FROM bank_income
GROUP BY Age, Sex, Job, Nationality;
