IF OBJECT_ID('urbansim.edges') IS NOT NULL
  DROP TABLE urbansim.edges
GO

CREATE TABLE urbansim.edges (
  from_node int NOT NULL
  ,to_node int NOT NULL
  ,travel_time_min float NOT NULL
)
GO

INSERT INTO urbansim.edges (from_node, to_node, travel_time_min)
SELECT 
  FNODE as [from]
  ,TNODE as [to]
  ,MIN((length * 60) / (
      CASE SPEED
        WHEN 0 THEN
          CASE SEGCLASS
            WHEN '2' THEN 55 --Highway
            WHEN '5' THEN 25 --Local Street
            WHEN '6' THEN 10 --Unpaved Road
            WHEN 'A' THEN 10 --Alley
            WHEN 'W' THEN 3  --Walkway
            WHEN 'Z' THEN 25 --Private Street
          END
        ELSE SPEED
    END  * 5280)) as [weight]
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


