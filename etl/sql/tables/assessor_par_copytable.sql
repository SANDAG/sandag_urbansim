SELECT * 
INTO spacecore.input.assessor_par 
FROM  OPENQUERY(sql2014b8, 'SELECT * FROM RM.dbo.ASSESSOR_PAR') 
