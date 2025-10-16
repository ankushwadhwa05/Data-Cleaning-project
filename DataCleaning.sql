SELECT *
FROM layoffs;
-- Remove Duplicates
-- Standardize the data
-- Null values or blank values
-- Remove any columns

CREATE TABLE layoffs_staging
like layoffs;
INSERT layoffs_staging
SELECT * from layoffs;

WITH DUPLIC AS (
SELECT *,ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as rn
FROM layoffs_staging )
SELECT * FROM DUPLIC
WHERE rn>1;
ALTER TABLE layoffs_staging
ADD rn INT ;
CREATE TABLE layoffs_staging2 AS
SELECT *,
       ROW_NUMBER() OVER(
         PARTITION BY company, location, industry, total_laid_off,
                      percentage_laid_off, `date`, stage, country, funds_raised_millions
         ORDER BY company
       ) AS ri
FROM layoffs_staging;

ALTER TABLE layoffs_staging2
DROP COLUMN rn;
DELETE
FROM layoffs_staging2
WHERE ri > 1;
-- standardizing
UPDATE layoffs_staging2
set company=trim(company);

SELECT distinct industry
from layoffs_staging2
order by 1 ;
SELECT *
FROM layoffs_staging2
where industry like 'Crypto%'; 

UPDATE layoffs_staging2
SET industry='Crypto'
where industry like 'Crypto%';

SELECT DISTINCT country
from layoffs_staging2;

UPDATE layoffs_staging2
SET country='United States'
where country like 'United States%';

SELECT DISTINCT country 
from layoffs_staging2;

SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')as datechange
from layoffs_staging2;

UPDATE layoffs_staging2
SET `date` =str_to_date(`date`,'%m/%d/%Y');

ALTER table layoffs_staging2
MODIFY COLUMN  `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off is null;

UPDATE layoffs_staging2
set industry=null
where industry='';

SELECT t1.industry,t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
WHERE (t1.industry is null or t1.industry='')
and t2.industry is not null;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company=t2.company
SET t1.industry = t2.industry
WHERE t1.industry is null
and t2.industry is not null;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off is null and percentage_laid_off is null;

ALTER TABLE layoffs_staging2
DROP COLUMN ri;

SELECT * 
FROM layoffs_staging2;  

