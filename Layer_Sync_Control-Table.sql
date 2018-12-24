USE [Carta]
GO

/****** Object:  Table [ADMINGTS].[Layer_Sync_Control]    Script Date: 6/8/2018 6:15:45 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [ADMINGTS].[Layer_Sync_Control](
	[ID] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[LayerName] [nvarchar](150) NOT NULL,
	[OwnerName] [nvarchar](50) NOT NULL,
	[LayerFullName]  AS (([OwnerName]+'.')+[LayerName]),
	[FeatureDataSet] [nvarchar](150) NULL,
	[SourceDB] [nvarchar](50) NOT NULL,
	[SourceDB_Type] [nvarchar](10) NOT NULL,
	[ScratchDB] [nvarchar](50) NULL,
	[ScratchDB_Type] [nvarchar](10) NULL,
	[TargetDB] [nvarchar](50) NOT NULL,
	[TargetDB_Type] [nvarchar](10) NOT NULL,
	[Process_Step] [int] NOT NULL,
	[Published] [nvarchar](1) NOT NULL,
	[Public] [nvarchar](1) NOT NULL,
	[Redacted] [nvarchar](1) NOT NULL,
	[Redact_Mask_OwnerName] [nvarchar](50) NULL,
	[Redact_Mask_LayerName] [nvarchar](150) NULL,
 CONSTRAINT [PK_admingts_Layer_Sync_Control] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [ADMINGTS].[Layer_Sync_Control] ADD  DEFAULT ('Prod') FOR [SourceDB_Type]
GO

ALTER TABLE [ADMINGTS].[Layer_Sync_Control] ADD  DEFAULT ('Prod') FOR [TargetDB_Type]
GO

ALTER TABLE [ADMINGTS].[Layer_Sync_Control] ADD  DEFAULT ((1)) FOR [Process_Step]
GO

ALTER TABLE [ADMINGTS].[Layer_Sync_Control] ADD  DEFAULT ('N') FOR [Published]
GO

ALTER TABLE [ADMINGTS].[Layer_Sync_Control] ADD  DEFAULT ('N') FOR [Public]
GO

ALTER TABLE [ADMINGTS].[Layer_Sync_Control] ADD  DEFAULT ('N') FOR [Redacted]
GO


