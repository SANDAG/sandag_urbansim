IF OBJECT_ID('ref.seg_class_id') IS NOT NULL
  DROP TABLE ref.seg_class_id
GO

CREATE TABLE ref.seg_class_id (
  seg_class_id nchar(1) not null primary key
  ,seg_class_desc nvarchar(25) not null
)

INSERT INTO ref.seg_class_id (seg_class_id, seg_class_desc) VALUES 
('1', 'Freeway'),
('2', 'State Route'),
('3', 'Major Roads'),
('4', 'Arterial'),
('5', 'Local Street'),
('6', 'Unpaved'),
('7', 'Private'),
('8', 'Freeway Ramp'),
('9', 'On-Ramp'),
('A', 'Alley'),
('H', 'Speed Hump'),
('M', 'Military Street'),
('P', 'Paper Street'),
('Q', 'Undocumented Street'),
('W', 'Walkway'),
('Z', 'Named Private Street')

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
  ,on_ramp bit NOT NULL
  ,geom geometry NOT NULL
)
GO

INSERT INTO urbansim.nodes (node, x, y, on_ramp, geom)
SELECT
  node
  ,avg(x) as x
  ,avg(y) as y
  ,0 as on_ramp --Dummy to assign all edges as not an on-ramp, update in the next statement
  ,geometry::Point(avg(x), avg(y), 2230) as geom
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

CREATE SPATIAL INDEX [ix_spatial_urbansim_nodes] ON [urbansim].[nodes]
(
	[geom]
)USING  GEOMETRY_GRID 
WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
GO

UPDATE n
  SET n.on_ramp = 1
FROM
  urbansim.nodes n
  INNER JOIN (SELECT FNODE FROM gis.roads WHERE SEGCLASS = '9') ons ON n.node = ons.FNODE
GO