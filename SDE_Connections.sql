USE [Carta]
GO

/****** Object:  Table [ADMINGTS].[SDE_Connections]    Script Date: 6/8/2018 6:15:12 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [ADMINGTS].[SDE_Connections](
	[ID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[SourceDB] [nvarchar](50) NOT NULL,
	[SourceDB_Type] [nvarchar](10) NOT NULL,
	[Data_Owner] [nvarchar](50) NOT NULL,
	[Conn_String] [nvarchar](100) NOT NULL,
 CONSTRAINT [PK_SDE_Connections] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


