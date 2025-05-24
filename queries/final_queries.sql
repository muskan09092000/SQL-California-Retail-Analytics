-- ===============================================
-- California Clothing Store Expansion SQL Analysis
-- Based on SafeGraph BigQuery Data
-- Author: Muskan Aggarwal
-- ===============================================

-- ===============================
-- SECTION 1: Identifying Counties with Expansion Potential
-- ===============================

-- 1.1 Count clothing stores per CA county
SELECT COUNT(location_name) AS number_of_stores,
       SUBSTRING(poi_cbg, 3, 3) AS county_sub,
       c.county
FROM `finalprojectmod.Finalmod.visits` a
INNER JOIN `finalprojectmod.Finalmod.brands` b
  ON a.safegraph_brand_ids = b.safegraph_brand_id
INNER JOIN `finalprojectmod.Finalmod.cbg_fips` c
  ON SUBSTRING(poi_cbg, 3, 3) = c.county_fips
WHERE poi_cbg LIKE '06%' AND b.top_category = 'Clothing Stores' AND c.state = 'CA'
GROUP BY county_sub, c.county
ORDER BY number_of_stores DESC;

-- 1.2 Find counties with no clothing stores
SELECT a.county, a.county_fips, b.number_of_stores
FROM `finalprojectmod.Finalmod.cbg_fips` a
LEFT JOIN `Final_MOD.Numberofstores_per_county_cali` b
  ON a.county_fips = b.county_sub
WHERE state = 'CA'
ORDER BY b.number_of_stores;

-- 1.3 High income population per county
SELECT SUM(`inc_125-150` + `inc_150-200` + `inc_gte200`) AS total_population_high_income,
       SUBSTRING(cbg, 2, 3) AS cbg_county_substring,
       b.county, b.state
FROM `finalprojectmod.Finalmod.cbg_demographics` a
INNER JOIN `finalprojectmod.Finalmod.cbg_fips` b
  ON SUBSTRING(a.cbg, 2, 3) = b.county_fips
WHERE cbg LIKE '6%' AND b.state = 'CA' AND LENGTH(cbg) = 11
GROUP BY cbg_county_substring, b.county, b.state
ORDER BY total_population_high_income DESC;

-- 1.4 Join to find counties with no stores but high income population
SELECT a.*, b.total_population_high_income
FROM `Final_MOD.Numberofstores_per_county_Cali_all` a
INNER JOIN `Final_MOD.Highincomepop_by_county_cali` b
  ON a.county_fips = b.cbg_county_substring
ORDER BY number_of_stores, total_population_high_income DESC;

-- ===============================
-- SECTION 2: Consumer Behavior Analysis
-- ===============================

-- 2.1 Foot traffic per day by county
SELECT SUBSTRING(poi_cbg, 3, 3) AS cbg_county_substring, c.county,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Monday') AS INT64))) AS avg_Monday,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Tuesday') AS INT64))) AS avg_Tuesday,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Wednesday') AS INT64))) AS avg_Wednesday,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Thursday') AS INT64))) AS avg_Thursday,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Friday') AS INT64))) AS avg_Friday,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Saturday') AS INT64))) AS avg_Saturday,
       ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(popularity_by_day, '$.Sunday') AS INT64))) AS avg_Sunday,
       ROUND(AVG(raw_visitor_counts)) AS raw_avg_visitor_counts
FROM `finalprojectmod.Finalmod.visits` a
INNER JOIN `finalprojectmod.Finalmod.brands` b
  ON a.safegraph_brand_ids = b.safegraph_brand_id
INNER JOIN `finalprojectmod.Finalmod.cbg_fips` c
  ON SUBSTRING(a.poi_cbg, 3, 3) = c.county_fips
WHERE poi_cbg LIKE '06%' AND b.top_category = 'Clothing Stores' AND c.state = 'CA'
GROUP BY cbg_county_substring, c.county
ORDER BY cbg_county_substring;

-- 2.2 Population per county
SELECT SUM(pop_total) AS population_per_county,
       SUBSTRING(cbg, 2, 3) AS cbg_county_substring,
       b.county
FROM `finalprojectmod.Finalmod.cbg_demographics` a
INNER JOIN `finalprojectmod.Finalmod.cbg_fips` b
  ON SUBSTRING(a.cbg, 2, 3) = b.county_fips
WHERE cbg LIKE '6%' AND LENGTH(cbg) = 11 AND b.state = 'CA'
GROUP BY cbg_county_substring, county
ORDER BY county;

-- 2.3 Stores per capita
SELECT a.*, b.population_per_county,
       (c.number_of_stores / b.population_per_county) AS number_of_stores_per_capita,
       c.number_of_stores
FROM `Final_MOD.Foot_traffic_by_county_cali` a
INNER JOIN `Final_MOD.Population_per_county_cali` b
  ON a.cbg_county_substring = b.cbg_county_substring
INNER JOIN `Final_MOD.Numberofstores_per_county_cali` c
  ON a.cbg_county_substring = c.county_sub
ORDER BY number_of_stores_per_capita DESC;

-- 2.4 Raw visitors per capita
SELECT a.*, b.population_per_county,
       (a.raw_avg_visitor_counts / b.population_per_county) AS avg_raw_visitors_per_capita,
       c.number_of_stores
FROM `Final_MOD.Foot_traffic_by_county_cali` a
INNER JOIN `Final_MOD.Population_per_county_cali` b
  ON a.cbg_county_substring = b.cbg_county_substring
INNER JOIN `Final_MOD.Numberofstores_per_county_cali` c
  ON a.cbg_county_substring = c.county_sub
ORDER BY avg_raw_visitors_per_capita DESC;

-- ===============================
-- SECTION 3: Specialized Store Expansion Opportunities
-- ===============================

-- --- CHILDREN & INFANTS STORES ANALYSIS ---

-- 3.1 Count of children's & infants' clothing stores by county
SELECT COUNT(location_name) AS number_of_stores,
       SUBSTRING(poi_cbg, 3, 3) AS county_sub
FROM `finalprojectmod.Finalmod.visits` a
JOIN `finalprojectmod.Finalmod.brands` b
  ON a.safegraph_brand_ids = b.safegraph_brand_id
WHERE poi_cbg LIKE '06%'
  AND b.top_category = 'Clothing Stores'
  AND b.sub_category = "Children's and Infants' Clothing Stores"
GROUP BY county_sub
ORDER BY county_sub;

-- 3.2 Population of children & infants per county
SELECT SUM(`pop_m_lt5` + `pop_f_lt5` + `pop_m_5-9` + `pop_m_10-14` + `pop_f_5-9` + `pop_f_10-14`) AS total_sum,
       SUBSTRING(cbg, 2, 3) AS cbg_county_substring,
       b.county,
       b.state
FROM `finalprojectmod.Finalmod.cbg_demographics` a
JOIN `finalprojectmod.Finalmod.cbg_fips` b
  ON SUBSTRING(a.cbg, 2, 3) = b.county_fips
WHERE cbg LIKE '6%' AND LENGTH(cbg) = 11 AND b.state = 'CA'
GROUP BY cbg_county_substring, b.county, b.state
ORDER BY total_sum;

-- 3.3 Per capita ratio for children's & infants' clothing stores
SELECT a.*, b.number_of_stores,
       (b.number_of_stores / total_sum) AS stores_per_capita_infant
FROM `Final_MOD.Child_infant_pop_per_county_cali` a
JOIN `Final_MOD.child_infant_stores_by_county` b
  ON a.cbg_county_substring = b.county_sub
ORDER BY stores_per_capita_infant;

-- --- WOMEN'S STORES ANALYSIS ---

-- 3.4 Count of women's clothing stores by county
SELECT COUNT(location_name) AS number_of_stores,
       SUBSTRING(poi_cbg, 3, 3) AS county_sub
FROM `finalprojectmod.Finalmod.visits` a
JOIN `finalprojectmod.Finalmod.brands` b
  ON a.safegraph_brand_ids = b.safegraph_brand_id
WHERE poi_cbg LIKE '06%'
  AND b.top_category = 'Clothing Stores'
  AND b.sub_category = "Women's Clothing Stores"
GROUP BY county_sub
ORDER BY county_sub;

-- 3.5 Population of women per county
SELECT SUM(pop_f_total) AS total_sum,
       SUBSTRING(cbg, 2, 3) AS cbg_county_substring,
       b.county,
       b.state
FROM `finalprojectmod.Finalmod.cbg_demographics` a
JOIN `finalprojectmod.Finalmod.cbg_fips` b
  ON SUBSTRING(a.cbg, 2, 3) = b.county_fips
WHERE cbg LIKE '6%' AND LENGTH(cbg) = 11 AND b.state = 'CA'
GROUP BY cbg_county_substring, b.county, b.state
ORDER BY total_sum;

-- 3.6 Per capita ratio for women's clothing stores
SELECT a.*, b.number_of_stores,
       (b.number_of_stores / total_sum) AS stores_per_capita_women
FROM `Final_MOD.Women_pop_per_county_cali` a
JOIN `Final_MOD.women_stores_by_county` b
  ON a.cbg_county_substring = b.county_sub
ORDER BY stores_per_capita_women;

-- --- MEN'S STORES ANALYSIS ---

-- 3.7 Count of men's clothing stores by county
SELECT COUNT(location_name) AS number_of_stores,
       SUBSTRING(poi_cbg, 3, 3) AS county_sub
FROM `finalprojectmod.Finalmod.visits` a
JOIN `finalprojectmod.Finalmod.brands` b
  ON a.safegraph_brand_ids = b.safegraph_brand_id
WHERE poi_cbg LIKE '06%'
  AND b.top_category = 'Clothing Stores'
  AND b.sub_category = "Men's Clothing Stores"
GROUP BY county_sub
ORDER BY county_sub;

-- 3.8 Population of men per county
SELECT SUM(pop_m_total) AS total_sum,
       SUBSTRING(cbg, 2, 3) AS cbg_county_substring,
       b.county,
       b.state
FROM `finalprojectmod.Finalmod.cbg_demographics` a
JOIN `finalprojectmod.Finalmod.cbg_fips` b
  ON SUBSTRING(a.cbg, 2, 3) = b.county_fips
WHERE cbg LIKE '6%' AND LENGTH(cbg) = 11 AND b.state = 'CA'
GROUP BY cbg_county_substring, b.county, b.state
ORDER BY total_sum;

-- 3.9 Per capita ratio for men's clothing stores
SELECT a.*, b.number_of_stores,
       (b.number_of_stores / total_sum) AS stores_per_capita_men
FROM `Final_MOD.men_pop_per_county_cali` a
JOIN `Final_MOD.men_stores_by_county` b
  ON a.cbg_county_substring = b.county_sub
ORDER BY stores_per_capita_men;
