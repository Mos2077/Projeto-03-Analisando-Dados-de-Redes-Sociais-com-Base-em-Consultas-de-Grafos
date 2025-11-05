// NOTE: The following script syntax is valid for database version 5.0 and above.

:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'Mental_Health_and_Social_Media_Balance_Dataset.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
CREATE CONSTRAINT `userId_UserMentalHealth_uniq` IF NOT EXISTS
FOR (n: `UserMentalHealth`)
REQUIRE (n.`userId`) IS UNIQUE;
CREATE CONSTRAINT `Age_Age_uniq` IF NOT EXISTS
FOR (n: `Age`)
REQUIRE (n.`Age`) IS UNIQUE;
CREATE CONSTRAINT `Gender_Gender_uniq` IF NOT EXISTS
FOR (n: `Gender`)
REQUIRE (n.`Gender`) IS UNIQUE;
CREATE CONSTRAINT `Daily_Screen_Time(hrs)_Daily_uniq` IF NOT EXISTS
FOR (n: `Daily`)
REQUIRE (n.`Daily_Screen_Time(hrs)`) IS UNIQUE;
CREATE CONSTRAINT `Sleep_Quality(1-10)_Sleep_uniq` IF NOT EXISTS
FOR (n: `Sleep`)
REQUIRE (n.`Sleep_Quality(1-10)`) IS UNIQUE;
CREATE CONSTRAINT `Stress_Level(1-10)_Stress_uniq` IF NOT EXISTS
FOR (n: `Stress`)
REQUIRE (n.`Stress_Level(1-10)`) IS UNIQUE;
CREATE CONSTRAINT `Days_Without_Social_Media_Days_uniq` IF NOT EXISTS
FOR (n: `Days`)
REQUIRE (n.`Days_Without_Social_Media`) IS UNIQUE;
CREATE CONSTRAINT `Exercise_Frequency(week)_Exercise_uniq` IF NOT EXISTS
FOR (n: `Exercise`)
REQUIRE (n.`Exercise_Frequency(week)`) IS UNIQUE;
CREATE CONSTRAINT `Social_Media_Platform_Social_Media_uniq` IF NOT EXISTS
FOR (n: `Social Media`)
REQUIRE (n.`Social_Media_Platform`) IS UNIQUE;
CREATE CONSTRAINT `Happiness_Index(1-10)_Happiness_uniq` IF NOT EXISTS
FOR (n: `Happiness`)
REQUIRE (n.`Happiness_Index(1-10)`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`User_ID` IN $idsToSkip AND NOT row.`User_ID` IS NULL
AND row.`Age` = 'U001'
CALL (row) {
  MERGE (n: `UserMentalHealth` { `userId`: row.`User_ID` })
  SET n.`userId` = row.`User_ID`
  SET n.`age` = toInteger(trim(row.`Age`))
  SET n.`gender` = row.`Gender`
  SET n.`dailyScreenTime` = toFloat(trim(row.`Daily_Screen_Time(hrs)`))
  SET n.`sleepQuality` = toFloat(trim(row.`Sleep_Quality(1-10)`))
  SET n.`stressLevel` = toFloat(trim(row.`Stress_Level(1-10)`))
  SET n.`daysWithoutSocialMedia` = toFloat(trim(row.`Days_Without_Social_Media`))
  SET n.`exerciseFrequency` = toFloat(trim(row.`Exercise_Frequency(week)`))
  SET n.`socialMediaPlatform` = row.`Social_Media_Platform`
  SET n.`happinessIndex` = toFloat(trim(row.`Happiness_Index(1-10)`))
  SET n.`User_ID` = row.`User_ID`
  SET n.`Age` = toInteger(trim(row.`Age`))
  SET n.`Gender` = row.`Gender`
  SET n.`Daily_Screen_Time(hrs)` = toFloat(trim(row.`Daily_Screen_Time(hrs)`))
  SET n.`Sleep_Quality(1-10)` = toFloat(trim(row.`Sleep_Quality(1-10)`))
  SET n.`Stress_Level(1-10)` = toFloat(trim(row.`Stress_Level(1-10)`))
  SET n.`Days_Without_Social_Media` = toFloat(trim(row.`Days_Without_Social_Media`))
  SET n.`Exercise_Frequency(week)` = toFloat(trim(row.`Exercise_Frequency(week)`))
  SET n.`Social_Media_Platform` = row.`Social_Media_Platform`
  SET n.`Happiness_Index(1-10)` = toFloat(trim(row.`Happiness_Index(1-10)`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Age` IN $idsToSkip AND NOT toInteger(trim(row.`Age`)) IS NULL
AND row.`Age` = '44'
CALL (row) {
  MERGE (n: `Age` { `Age`: toInteger(trim(row.`Age`)) })
  SET n.`Age` = toInteger(trim(row.`Age`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Gender` IN $idsToSkip AND NOT row.`Gender` IS NULL
AND row.`Gender` = 'Male'
CALL (row) {
  MERGE (n: `Gender` { `Gender`: row.`Gender` })
  SET n.`Gender` = row.`Gender`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Daily_Screen_Time(hrs)` IN $idsToSkip AND NOT toFloat(trim(row.`Daily_Screen_Time(hrs)`)) IS NULL
AND row.`Daily_Screen_Time(hrs)` = '3.1'
CALL (row) {
  MERGE (n: `Daily` { `Daily_Screen_Time(hrs)`: toFloat(trim(row.`Daily_Screen_Time(hrs)`)) })
  SET n.`Daily_Screen_Time(hrs)` = toFloat(trim(row.`Daily_Screen_Time(hrs)`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Sleep_Quality(1-10)` IN $idsToSkip AND NOT toFloat(trim(row.`Sleep_Quality(1-10)`)) IS NULL
AND row.`Sleep_Quality(1-10)` = '7.0'
CALL (row) {
  MERGE (n: `Sleep` { `Sleep_Quality(1-10)`: toFloat(trim(row.`Sleep_Quality(1-10)`)) })
  SET n.`Sleep_Quality(1-10)` = toFloat(trim(row.`Sleep_Quality(1-10)`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Stress_Level(1-10)` IN $idsToSkip AND NOT toFloat(trim(row.`Stress_Level(1-10)`)) IS NULL
AND row.`Stress_Level(1-10)` = '6.0'
CALL (row) {
  MERGE (n: `Stress` { `Stress_Level(1-10)`: toFloat(trim(row.`Stress_Level(1-10)`)) })
  SET n.`Stress_Level(1-10)` = toFloat(trim(row.`Stress_Level(1-10)`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Days_Without_Social_Media` IN $idsToSkip AND NOT toFloat(trim(row.`Days_Without_Social_Media`)) IS NULL
AND row.`Days_Without_Social_Media` = '2.0'
CALL (row) {
  MERGE (n: `Days` { `Days_Without_Social_Media`: toFloat(trim(row.`Days_Without_Social_Media`)) })
  SET n.`Days_Without_Social_Media` = toFloat(trim(row.`Days_Without_Social_Media`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Exercise_Frequency(week)` IN $idsToSkip AND NOT toFloat(trim(row.`Exercise_Frequency(week)`)) IS NULL
AND row.`Exercise_Frequency(week)` = '5.0'
CALL (row) {
  MERGE (n: `Exercise` { `Exercise_Frequency(week)`: toFloat(trim(row.`Exercise_Frequency(week)`)) })
  SET n.`Exercise_Frequency(week)` = toFloat(trim(row.`Exercise_Frequency(week)`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Social_Media_Platform` IN $idsToSkip AND NOT row.`Social_Media_Platform` IS NULL
AND row.`Social_Media_Platform` = 'Facebook'
CALL (row) {
  MERGE (n: `Social Media` { `Social_Media_Platform`: row.`Social_Media_Platform` })
  SET n.`Social_Media_Platform` = row.`Social_Media_Platform`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`Happiness_Index(1-10)` IN $idsToSkip AND NOT toFloat(trim(row.`Happiness_Index(1-10)`)) IS NULL
AND row.`Happiness_Index(1-10)` = '10.0'
CALL (row) {
  MERGE (n: `Happiness` { `Happiness_Index(1-10)`: toFloat(trim(row.`Happiness_Index(1-10)`)) })
  SET n.`Happiness_Index(1-10)` = toFloat(trim(row.`Happiness_Index(1-10)`))
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`User_ID` = 'U001'
CALL (row) {
  MATCH (source: `UserMentalHealth` { `userId`: row.`User_ID` })
  MATCH (target: `Age` { `Age`: toInteger(trim(row.`Age`)) })
  MERGE (source)-[r: `U001`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Age` = '44'
CALL (row) {
  MATCH (source: `Age` { `Age`: toInteger(trim(row.`Age`)) })
  MATCH (target: `Gender` { `Gender`: row.`Gender` })
  MERGE (source)-[r: `44`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Gender` = 'Male'
CALL (row) {
  MATCH (source: `Gender` { `Gender`: row.`Gender` })
  MATCH (target: `Daily` { `Daily_Screen_Time(hrs)`: toFloat(trim(row.`Daily_Screen_Time(hrs)`)) })
  MERGE (source)-[r: `Male`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Daily_Screen_Time(hrs)` = '3.1'
CALL (row) {
  MATCH (source: `Daily` { `Daily_Screen_Time(hrs)`: toFloat(trim(row.`Daily_Screen_Time(hrs)`)) })
  MATCH (target: `Sleep` { `Sleep_Quality(1-10)`: toFloat(trim(row.`Sleep_Quality(1-10)`)) })
  MERGE (source)-[r: `3.1`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Daily_Screen_Time(hrs)` = '7.0'
CALL (row) {
  MATCH (source: `Sleep` { `Sleep_Quality(1-10)`: toFloat(trim(row.`Sleep_Quality(1-10)`)) })
  MATCH (target: `Stress` { `Stress_Level(1-10)`: toFloat(trim(row.`Stress_Level(1-10)`)) })
  MERGE (source)-[r: `7.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Stress_Level(1-10)` = '6.0'
CALL (row) {
  MATCH (source: `Stress` { `Stress_Level(1-10)`: toFloat(trim(row.`Stress_Level(1-10)`)) })
  MATCH (target: `Days` { `Days_Without_Social_Media`: toFloat(trim(row.`Days_Without_Social_Media`)) })
  MERGE (source)-[r: `6.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Days_Without_Social_Media` = '2.0'
CALL (row) {
  MATCH (source: `Days` { `Days_Without_Social_Media`: toFloat(trim(row.`Days_Without_Social_Media`)) })
  MATCH (target: `Exercise` { `Exercise_Frequency(week)`: toFloat(trim(row.`Exercise_Frequency(week)`)) })
  MERGE (source)-[r: `2.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Exercise_Frequency(week)` = '2.0'
CALL (row) {
  MATCH (source: `Exercise` { `Exercise_Frequency(week)`: toFloat(trim(row.`Exercise_Frequency(week)`)) })
  MATCH (target: `Social Media` { `Social_Media_Platform`: row.`Social_Media_Platform` })
  MERGE (source)-[r: `2.0`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
WHERE row.`Social_Media_Platform` = 'Facebook'
CALL (row) {
  MATCH (source: `Social Media` { `Social_Media_Platform`: row.`Social_Media_Platform` })
  MATCH (target: `Happiness` { `Happiness_Index(1-10)`: toFloat(trim(row.`Happiness_Index(1-10)`)) })
  MERGE (source)-[r: `facebook`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
