IF OBJECT_ID('urbansim.edges') IS NOT NULL
  DROP TABLE urbansim.edges
GO

CREATE TABLE urbansim.edges (
  from_node int NOT NULL
  ,to_node int NOT NULL
  ,distance float NOT NULL
)
GO

INSERT INTO urbansim.edges (from_node, to_node, distance)
SELECT 
  FNODE as [from]
  ,TNODE as [to]
  ,min([length]) as distance
FROM 
  GIS.roads 
WHERE SEGCLASS NOT IN ('P')
GROUP BY FNODE, TNODE
GO

ALTER TABLE urbansim.edges ADD CONSTRAINT pk_urbansim_edges_from_to PRIMARY KEY CLUSTERED (from_node, to_node)
GO


IF OBJECT_ID('urbansim.nodes') IS NOT NULL
  DROP TABLE urbansim.nodes
GO

CREATE TABLE urbansim.nodes (
  node int NOT NULL
  ,x float NOT NULL
  ,y float NOT NULL
)
GO

INSERT INTO urbansim.nodes (node, x, y)
SELECT
  node
  ,avg(x) as x
  ,avg(y) as y
FROM (
SELECT
  FNODE as node
  ,FRXCOORD as x
  ,FRYCOORD as y
FROM 
  GIS.roads 
WHERE SEGCLASS NOT IN ('P')
UNION ALL
SELECT
  TNODE as node
  ,TOXCOORD as x
  ,TOYCOORD as y
FROM 
  GIS.roads 
WHERE SEGCLASS NOT IN ('P')) nodes
GROUP BY node
GO

ALTER TABLE urbansim.nodes ADD CONSTRAINT pk_urbansim_nodes_node PRIMARY KEY CLUSTERED (node)
GO


