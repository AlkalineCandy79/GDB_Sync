USE [Carta]
GO

/****** Object:  View [ADMINGTS].[View_Layers_To_Update]    Script Date: 6/8/2018 5:53:21 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [ADMINGTS].[View_Layers_To_Update]
AS

select Schema_Name([schema_id]) as 'Data_Owner', x.name as 'Table_Name', z.name as 'SDE_Name', y.FC_Type as 'Type', x.create_date as 'Date_Created', x.modify_date as 'Date_Last_Modified'
from sys.tables as x
inner join [ADMINGTS].[View_Find_Layers] as y
on x.name = y.[Table_Name] and Schema_Name([schema_id]) = y.Schema_Name
inner join [sde].[GDB_ITEMS] as z
on (Schema_Name + '.' + y.[Table_Name]) = right(z.name, len(z.name) - charindex('.', z.name))
where cast(x.modify_date as date) = CAST(CURRENT_TIMESTAMP AS DATE)


GO


