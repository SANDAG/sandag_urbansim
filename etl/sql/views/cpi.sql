IF OBJECT_ID('ref.cpi_sd') IS NOT NULL
    DROP TABLE ref.cpi_non_shelter_sd;

CREATE TABLE ref.cpi_sd (
  yr smallint NOT NULL PRIMARY KEY
  ,cpi_u float NOT NULL
  ,cpi_non_shelter float NOT NULL 
)

-- CPI_U SOURCE: BLS Series ID: CUURA424SA0, CPI All Urban Conusumers, San Diego, CA
-- CPI NON SHELTER SOURCE: BLS Series ID: CUURA424SA0L2, CPI All Urban Consumers Less Shelter, San Diego, CA

INSERT INTO 
  ref.cpi_sd (yr, cpi_u, cpi_non_shelter)
VALUES
  (1980,79.4,82.2)
  ,(1981,90.1,89.4)
  ,(1982,96.2,95.1)
  ,(1983,99,100.1)
  ,(1984,104.8,104.8)
  ,(1985,110.4,107.9)
  ,(1986,113.5,108.9)
  ,(1987,117.5,112.7)
  ,(1988,123.4,118.6)
  ,(1989,130.6,126.3)
  ,(1990,138.4,133.6)
  ,(1991,143.4,138.3)
  ,(1992,147.4,142.5)
  ,(1993,150.6,146.8)
  ,(1994,154.5,152)
  ,(1995,156.8,155.1)
  ,(1996,160.9,158.1)
  ,(1997,163.7,160)
  ,(1998,166.9,160.7)
  ,(1999,172.8,164.8)
  ,(2000,182.8,173.8)
  ,(2001,191.2,179.8)
  ,(2002,197.9,182.4)
  ,(2003,205.3,186.6)
  ,(2004,212.8,191.8)
  ,(2005,220.6,198.7)
  ,(2006,228.1,205)
  ,(2007,233.321,208.807)
  ,(2008,242.313,217.005)
  ,(2009,242.27,215.905)
  ,(2010,245.464,221.675)
  ,(2011,252.91,232.106)
  ,(2012,256.961,235.377)
  ,(2013,260.317,237.371)
  ,(2014,265.145,241.32)
  ,(2015,269.436,242.093)