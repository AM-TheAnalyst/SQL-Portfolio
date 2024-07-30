SELECT * FROM layoffs;
-- 	It's best practice to always try and create a copy of your raw data as back up. Work with new table.
CREATE TABLE layoffs_staging LIKE layoffs;
INSERT INTO layoffs_staging SELECT * FROM layoffs;
SELECT *,ROW_NUMBER () OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) AS row_num FROM layoffs_staging;

WITH RECURSIVE CTE AS
		(SELECT *,ROW_NUMBER () OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) AS row_num FROM layoffs_staging)
SELECT * FROM CTE WHERE row_num >1;

SELECT * FROM layoffs WHERE company='Casper';
SELECT * FROM layoffs_staging;

CREATE TEMPORARY TABLE layoff_staging2 SELECT *,ROW_NUMBER () OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) AS row_num2 FROM layoffs_staging;
SELECT * FROM layoff_staging2 WHERE row_num2 >1;
DELETE FROM layoff_staging2 WHERE row_num2>1;
CREATE TABLE layoffs_staging2 LIKE layoff_staging2;
INSERT INTO layoffs_staging2 SELECT * FROM layoff_staging2;
SELECT * FROM layoffs_staging2;
SELECT * FROM layoff_staging2;

SELECT company, TRIM(company) FROM layoffs_staging2;
UPDATE layoffs_staging2 SET company= TRIM (company);
SELECT DISTINCT (industry) FROM layoffs_staging2 ORDER BY 1;
SELECT * FROM layoffs_staging2 WHERE industry LIKE 'crypto%';
UPDATE layoffs_staging2 SET industry= 'Crypto' WHERE industry LIKE 'crypto%';
SELECT DISTINCT (country) FROM layoffs_staging2 ORDER BY 1;
-- Not a good move DELETE layoffs_staging2 WHERE country= 'United States.';
SELECT * FROM layoffs_staging2 WHERE country='united states.';
UPDATE layoffs_staging2 SET country= 'United States' WHERE country LIKE 'United States%';

SELECT `date`, STR_TO_DATE(`date`,'%m/%d/%Y') FROM layoffs_staging2;
UPDATE layoffs_staging2 SET `date`=STR_TO_DATE(`date`,'%m/%d/%Y');
SELECT `date` FROM layoffs_staging2;
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry= '';
SELECT * FROM layoffs_staging2 WHERE company= 'Airbnb';

UPDATE layoffs_staging2 SET industry=NULL WHERE industry='';

SELECT * FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company AND t1.location=t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT t1.industry,t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company AND t1.location=t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company AND t1.location=t2.location
SET t1.industry=t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
DELETE FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;
ALTER TABLE layoffs_staging2 DROP COLUMN row_num2;









