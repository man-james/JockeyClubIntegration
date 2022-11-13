/****** Object:  Table [dbo].[serviceHours]    Script Date: 11/11/2022 6:48:36 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[serviceHours](
	[occurrenceId] [nvarchar](50) NOT NULL,
	[volunteerId] [nvarchar](50) NOT NULL,
	[startDate] [datetime] NOT NULL,
	[endDate] [datetime] NULL,
	[hours] [numeric](18, 0) NOT NULL,
	[status] [varchar](16) NOT NULL,
	[xml] [nvarchar](max) NOT NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

