CREATE TABLE CovidVacc (
    iso_code	VARCHAR(512),
    continent	VARCHAR(512),
    location	VARCHAR(512),
    date	DATE NOT NULL,
    new_tests	VARCHAR(512),
    total_tests	VARCHAR(512),
    total_tests_per_thousand	VARCHAR(512),
    new_tests_per_thousand	VARCHAR(512),
    new_tests_smoothed	VARCHAR(512),
    new_tests_smoothed_per_thousand	VARCHAR(512),
    positive_rate	VARCHAR(512),
    tests_per_case	VARCHAR(512),
    tests_units	VARCHAR(512),
    total_vaccinations	VARCHAR(512),
    people_vaccinated	VARCHAR(512),
    people_fully_vaccinated	VARCHAR(512),
    new_vaccinations	VARCHAR(512),
    new_vaccinations_smoothed	VARCHAR(512),
    total_vaccinations_per_hundred	VARCHAR(512),
    people_vaccinated_per_hundred	VARCHAR(512),
    people_fully_vaccinated_per_hundred	VARCHAR(512),
    new_vaccinations_smoothed_per_million	VARCHAR(512),
    stringency_index	DOUBLE PRECISION,
    population_density	DOUBLE PRECISION,
    median_age	DOUBLE PRECISION,
    aged_65_older	DOUBLE PRECISION,
    aged_70_older	DOUBLE PRECISION,
    gdp_per_capita	DOUBLE PRECISION,
    extreme_poverty	VARCHAR(512),
    cardiovasc_death_rate	DOUBLE PRECISION,
    diabetes_prevalence	DOUBLE PRECISION,
    female_smokers	VARCHAR(512),
    male_smokers	VARCHAR(512),
    handwashing_facilities	DOUBLE PRECISION,
    hospital_beds_per_thousand	DOUBLE PRECISION,
    life_expectancy	DOUBLE PRECISION,
    human_development_index	DOUBLE PRECISION
);

SELECT  * from CovidVacc

---creating table for covid deaths
CREATE TABLE CovidDeaths (
    iso_code	VARCHAR(512),
    continent	VARCHAR(512),
    location	VARCHAR(512),
    date	DATE NOT NULL,
    total_cases	INT,
    new_cases	INT,
    new_cases_smoothed	DOUBLE PRECISION,
    total_deaths	INT,
    new_deaths	INT,
    new_deaths_smoothed	DOUBLE PRECISION,
    total_cases_per_million	DOUBLE PRECISION,
    new_cases_per_million	DOUBLE PRECISION,
    new_cases_smoothed_per_million	DOUBLE PRECISION,
    total_deaths_per_million	DOUBLE PRECISION,
    new_deaths_per_million	DOUBLE PRECISION,
    new_deaths_smoothed_per_million	DOUBLE PRECISION,
    reproduction_rate	DOUBLE PRECISION,
    icu_patients	VARCHAR(512),
    icu_patients_per_million	VARCHAR(512),
    hosp_patients	VARCHAR(512),
    hosp_patients_per_million	VARCHAR(512),
    weekly_icu_admissions	VARCHAR(512),
    weekly_icu_admissions_per_million	VARCHAR(512),
    weekly_hosp_admissions	VARCHAR(512),
    weekly_hosp_admissions_per_million	VARCHAR(512),
    new_tests	VARCHAR(512),
    total_tests	VARCHAR(512),
    total_tests_per_thousand	VARCHAR(512),
    new_tests_per_thousand	VARCHAR(512),
    new_tests_smoothed	VARCHAR(512),
    new_tests_smoothed_per_thousand	VARCHAR(512),
    positive_rate	VARCHAR(512),
    tests_per_case	VARCHAR(512),
    tests_units	VARCHAR(512),
    total_vaccinations	VARCHAR(512),
    people_vaccinated	VARCHAR(512),
    people_fully_vaccinated	VARCHAR(512),
    new_vaccinations	VARCHAR(512),
    new_vaccinations_smoothed	VARCHAR(512),
    total_vaccinations_per_hundred	VARCHAR(512),
    people_vaccinated_per_hundred	VARCHAR(512),
    people_fully_vaccinated_per_hundred	VARCHAR(512),
    new_vaccinations_smoothed_per_million	VARCHAR(512),
    stringency_index	DOUBLE PRECISION,
    population	BIGINT,
    population_density	DOUBLE PRECISION,
    median_age	DOUBLE PRECISION,
    aged_65_older	DOUBLE PRECISION,
    aged_70_older	DOUBLE PRECISION,
    gdp_per_capita	DOUBLE PRECISION,
    extreme_poverty	VARCHAR(512),
    cardiovasc_death_rate	DOUBLE PRECISION,
    diabetes_prevalence	DOUBLE PRECISION,
    female_smokers	VARCHAR(512),
    male_smokers	VARCHAR(512),
    handwashing_facilities	DOUBLE PRECISION,
    hospital_beds_per_thousand	DOUBLE PRECISION,
    life_expectancy	DOUBLE PRECISION,
    human_development_index	DOUBLE PRECISION
);

SELECT  * from CovidDeaths limit 2;

SELECT location, date, total_cases,new_cases, total_deaths, population
from CovidDeaths
order by 1,2

--looking at Total cases verses Total Deaths
---one of the interger columns have to changed to float using CASt
--- PostGresql perfoprms interger dividion snd truncates the decimal part of a division value
SELECT location, date, total_cases,total_deaths, 
(CAST(total_deaths AS FLOAT) /total_cases)*100 as DeathPercentage
from CovidDeaths
where location like '%Nigeria'
order by 1,2
--looking at countires with the highest infection rate
SELECT location, Population, Max(total_cases) as HighInfectionCount,
Max((CAST(total_cases as Float)/Population))*100 as Pop_Percent
from CovidDeaths
--where location like '%states%'
group by location, Population
order by Pop_percent desc

-- showing countires with the highest death count per population
SELECT location, Max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
--where location like '%states%'
where continent is not null
group by location
order by TotalDeathCount desc

--- Let's break it down by continent
SELECT continent, Max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
--where location like '%states%'
where continent is not null
group by continent
order by TotalDeathCount desc;

SELECT location, Max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
--where location like '%states%'
where continent is  null
group by location
order by TotalDeathCount desc

-- Global Numbers
SELECT  SUM(new_cases)  TotalNewCases, SUM(CAST(new_deaths AS INT))  TotalNewDeaths, 
SUM(CAST(new_deaths AS INT)) /SUM(new_cases)*100 as DeathPercentage
from CovidDeaths
where continent is not null
--group by date
order by 1,2

SELECT * 
FROM CovidVacc
--JOINING THE TWO TABLES ON LOCATION AND DATE
SELECT * 
FROM CovidDeaths  dea
JOIN CovidVacc  vac
ON dea.location = vac.location
and dea.date = vac.date
---Total vaccination vs polpulation
select dea.continent, dea.location,dea.date,dea.population, vac.new_vaccinations
FROM CovidDeaths  dea
JOIN CovidVacc  vac
ON dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
order by 2,3

--Partition by 
select dea.continent, dea.location,dea.date,dea.population, vac.new_vaccinations,
SUM(CAST(vac.new_vaccinations AS INT)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as 
RollingPeopleVacc
FROM CovidDeaths  dea
JOIN CovidVacc  vac
ON dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
order by 2,3

--CTE
WITH ROLLINPVAC AS 
(
	select dea.continent, dea.location,dea.date,dea.population, vac.new_vaccinations,
	SUM(CAST(vac.new_vaccinations AS INT)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as 
	RollingPeopleVacc
	FROM CovidDeaths  dea
	JOIN CovidVacc  vac
	ON dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null
	--order by 2,3
)
SELECT *,(RollingPeopleVacc/CAST(Population AS DOUBLE PRECISION))*100
FROM ROLLINPVAC

--TEMP TABLE
DROP TABLE PERCENTPOPVAC2;
CREATE TEMP TABLE PERCENTPOPVAC2(

Continent varchar(255),
Location varchar(255),
date Varchar(255),
population numeric,
New_vaccination BIGINT,
RollingPeopleVacc BIGINT);

INSERT INTO PERCENTPOPVAC2(

select dea.continent, dea.location,dea.date,dea.population, CAST(vac.new_vaccinations AS BIGINT),
	SUM(CAST(vac.new_vaccinations AS BIGINT)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as 
	RollingPeopleVacc
	FROM CovidDeaths  dea
	JOIN CovidVacc  vac
	ON dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null

);
SELECT *,(RollingPeopleVacc/CAST(Population AS DOUBLE PRECISION))*100
FROM PERCENTPOPVAC2

--CREATING VIEWS
CREATE VIEW PERCENTPOPVAC2  AS select dea.continent, dea.location,dea.date,dea.population, CAST(vac.new_vaccinations AS BIGINT),
	SUM(CAST(vac.new_vaccinations AS BIGINT)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) as 
	RollingPeopleVacc
	FROM CovidDeaths  dea
	JOIN CovidVacc  vac
	ON dea.location = vac.location
	and dea.date = vac.date
	where dea.continent is not null

CREATE VIEW VAC_POP AS
select dea.continent, dea.location,dea.date,dea.population, vac.new_vaccinations
FROM CovidDeaths  dea
JOIN CovidVacc  vac
ON dea.location = vac.location
and dea.date = vac.date
where dea.continent is not null
order by 2,3

CREATE VIEW HighDeath_Pop AS 
SELECT location, Max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
--where location like '%states%'
where continent is not null
group by location
order by TotalDeathCount desc

