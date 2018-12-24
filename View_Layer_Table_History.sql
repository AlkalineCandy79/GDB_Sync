USE [Carta]
GO

/****** Object:  View [ADMINGTS].[View_Layer_Table_History]    Script Date: 6/8/2018 6:00:57 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [ADMINGTS].[View_Layer_Table_History]
AS

select distinct name as 'Table_Name', Schema_Name([schema_id]) as 'Schema',y.FC_Type as 'Type', x.create_date as 'Date_Created', x.modify_date as 'Date_Last_Modified'
from sys.tables as x
inner join [ADMINGTS].[View_Find_Layers] as y
on x.name = y.[Table_Name]

GO


