/****** Object:  Table [dbo].[serviceHours]    Script Date: 9/5/2023 8:02:44 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[registrations](
	[hohkId] [nvarchar](50) NOT NULL,
	[jcvarId] [nvarchar](128) NOT NULL,
	[status] [varchar](16) NOT NULL,
	[xml] [nvarchar](max) NOT NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[error] [varchar](128) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

