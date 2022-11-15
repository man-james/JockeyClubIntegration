/****** Object:  Table [dbo].[occurrences]    Script Date: 15/11/2022 10:50:12 am ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[occurrences](
	[occurrenceId] [nvarchar](50) NOT NULL,
	[status] [varchar](16) NOT NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
 CONSTRAINT [PK_occurrences] PRIMARY KEY CLUSTERED 
(
	[occurrenceId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


