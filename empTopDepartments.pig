set job.queuename 'Top 3 Employee Departments';

-- Load EMployee 
empRel = LOAD 'hdfs://nameservice1/user/piguser/Pig/emp.csv' USING PigStorage(',') AS (empId:int,name:chararray,dept:chararray,sal:float);

-- Group Employee By Department 
empGroup = GROUP empRel By dept;

-- Get Count for each Department 
countDept = FOREACH empGroup GENERATE group, COUNT(empRel.empId) AS total;

-- Filter the Department by count
filteredDept = FILTER countDept BY total > 1L;

-- Sort the Output 
sortedDeptCount = ORDER filteredDept BY total DESC;

-- Sort the Output 
finalCount = LIMIT sortedDeptCount 3;

-- STORE THE OUTPUT 
STORE finalCount INTO 'Pig/Output/TopDeptments' USING PigStorage(',');