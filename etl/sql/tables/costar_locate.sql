SELECT [property_id]
	,[building_address]
	,[building_class]
	,[building_location]
	,[building_name]
	,[building_park]
	,[building_status]
	,[city]
	,[latitude]
	,[longitude]
	,[centroid]
	,[parcel_id]
	,[subparcel_id]
INTO [spacecore].[input].[costar_locate]
FROM [spacecore].[input].[costar]
WHERE parcel_id IS NULL

--ADD ID TO USE AS OBJECT IN SPATIAL VIEWER
ALTER TABLE [spacecore].[input].[costar_locate] 
ADD id  int IDENTITY(1,1)
