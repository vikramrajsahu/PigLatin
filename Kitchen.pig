--Set Pig Job Queue name
set job.name 'Favourite Food';

-- Load data
kitchen = LOAD 'Pig/kitchen.json' USING JsonLoader('food:chararray,person:chararray,amount:int');

-- Group by food
groupDish = GROUP kitchen BY food;

-- Count the food in each group
countFood = FOREACH groupDish GENERATE group AS food, COUNT(kitchen.food) AS totalCount;

-- Get favourite food
favouriteFood = FILTER countFood BY totalCount > 1L;

-- Store the result
STORE favouriteFood INTO 'Pig/Output/FavouriteFood' USING PigStorage(',');