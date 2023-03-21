/****** Object:  Table [dbo].[serviceHours]    Script Date: 21/12/2022 10:43:20 pm ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[serviceHours](
	[occurrenceId] [nvarchar](50) NOT NULL,
	[volunteerId] [nvarchar](128) NOT NULL,
	[startDate] [datetime] NOT NULL,
	[endDate] [datetime] NULL,
	[hours] [numeric](18, 0) NOT NULL,
	[status] [varchar](16) NOT NULL,
	[xml] [nvarchar](max) NOT NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[error] [varchar](128) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


