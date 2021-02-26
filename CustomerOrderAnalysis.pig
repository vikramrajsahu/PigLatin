set job.name '$job_name';

-- Load customer relation
customers = LOAD 'hdfs://nameservice1/user/edureka_788309/Pig/customers.csv' USING PigStorage(',') AS (custId:int,cname:chararray,age:int,city:chararray,balance:float);

-- Load order relation
orders = LOAD 'hdfs://nameservice1/user/edureka_788309/Pig/orders.csv' USING PigStorage(',') AS (orderId:int,orderDate:chararray,custId:int,orderValue:float);

-- Get total order value for the customers
groupOrders = GROUP orders BY custId;

-- Get total
orderCust = FOREACH groupOrders GENERATE group AS custId, ROUND(SUM(orders.orderValue)) as total;

-- Join customer and orders relation
joinRelation = JOIN orderCust BY custId LEFT OUTER, customers BY custId;

final = FOREACH joinRelation GENERATE orderCust::custId, customers::cname, orderCust::total;

-- Store the result
STORE final INTO 'Pig/Output/CustomerTotalSales' USING PigStorage(',');

-- Command to use
-- pig -param job_name='Total Customer Sales' Pig/CustomerOrderAnalysis.pig

