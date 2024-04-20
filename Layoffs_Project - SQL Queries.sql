create database world_layoffs

use world_layoffs

 select * from layoffs

 ----------------------------------------------------------------------------------------------------------------------------------------------
 -------------------------------------------------------- 1. REMOVE DUPLICATES ---------------------------------------------------------------
 ----------------------------------------------------------------------------------------------------------------------------------------------
 
 -- CREATE ANOTHER DUPLICATE TABLE OF LAYOFFS BECAUSE WE CAN CHANGE A LOT OF DATA SO IF MISTAKES WE SHOULD HAVE RAW DATA
create table layoffs_staging like layoffs  ;-- create column only
insert layoffs_staging select * from layoffs ;-- create whole table
select * from layoffs_staging ;


----- CHECK DATA HAVE DUPLICATES OR NOT 
-- Step 1.
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging
where row_num > 1

-- Step 2. (with subquery)
select * from (select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging) as a 
where row_num > 1


-- Step 2. ( or with cte)
with duplicate_cte as 
(select * from (select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging) as a 
) 
select * from duplicate_cte where row_num > 1 ;


-- CREATE ANOTHER TABLE FOR DELETING DUPLICATES VALUE

Create table layoffs_staging2 (
company text,
location text,
industry text,
total_laid_off int default null,
percentage_laid_off text,
date text,
stage text, 
country text,
funds_raised_millions int default null,
row_num int) ;

select * from layoffs_staging2 ;

insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
from layoffs_staging ;


select * from layoffs_staging2 
where row_num > 1;

-- DELETE THE DUPLICATE VALUES
DELETE from layoffs_staging2 
where row_num > 1;


--------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------- 2. STANDARDIZE THE DATA --------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1.
Select company, trim(company) from layoffs_staging2;

update layoffs_staging2 set company = trim(company);

Select distinct industry from layoffs_staging2
order by industry ;

-- Step 2. 
select * from layoffs_staging2 where industry like 'crypto%';

update layoffs_staging2 set industry = 'Crypto' where industry like 'crypto%';
select * from layoffs_staging2 where industry = 'cryptocurrency';

-- Step 3.
select distinct country, trim(trailing '.' from country ) from layoffs_staging2;
update layoffs_staging2 set country = trim(trailing '.' from country) where country like 'United States';


-- Step 4.
select date, str_to_date(date, '%m/%d/%Y') from layoffs_staging2 ;
update layoffs_staging2 set date = str_to_date(date, '%m/%d/%Y') ;
alter table layoffs_staging2 modify date date ;       -- convert text to date datatype  

----------------------------------------------------------------------------------------------------------------------------------------------
 -------------------------------------------------- 3. NULL VALUE OR BLANK VALUE ---------------------------------------------------------------
 ----------------------------------------------------------------------------------------------------------------------------------------------
-- Step 1.
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

update layoffs_staging2 set industry = null where industry = '';

select *  from layoffs_staging2 where industry is null  or industry = '';

select * from layoffs_staging2 where company = 'Airbnb';

select t1.industry, t2.industry from layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company = t2.company 
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

update layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company = t2.company
set t1.industry = t2.industry where t1.industry is null and t2.industry is not null ;

select * from layoffs_staging2 where company like 'Bally%';

-- Step 2.
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

----------------------------------------------------------------------------------------------------------------------------------------------
 ----------------------------------------------- 4. REMOVE ANY COLUMNS OR ROWS---------------------------------------------------------------
 ----------------------------------------------------------------------------------------------------------------------------------------------
 

delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2
drop column  row_num;


