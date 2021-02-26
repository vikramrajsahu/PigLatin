set job.queuename 'Statistic of Employee';

-- Load EMployee
empRel = LOAD 'hdfs://nameservice1/user/piguser/Pig/emp.csv' USING PigStorage(',') AS (empId:int,name:chararray,dept:chararray,sal:float);

-- Group Employee By Department
empGroup = GROUP empRel ALL;

-- Get Average Sal
avgSal = FOREACH empGroup GENERATE 'Average', ROUND(AVG(empRel.sal)) AS sal;

-- Get Total Sal
totalSal = FOREACH empGroup GENERATE 'Total', ROUND(SUM(empRel.sal)) AS sal;

-- Get Max Sal
maxSal = FOREACH empGroup GENERATE 'Max', ROUND(MAX(empRel.sal)) AS sal;

-- Get Min Sal
minSal = FOREACH empGroup GENERATE 'Min', ROUND(MIN(empRel.sal)) AS sal;

-- Combine the relations
summaryEmp = UNION avgSal,totalSal,maxSal,minSal;

-- STORE THE OUTPUT
STORE summaryEmp INTO 'Pig/Output/SalaryStatistics' USING PigStorage(',');