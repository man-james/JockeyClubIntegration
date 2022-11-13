/****** Object:  Table [dbo].[occurrences]    Script Date: 31/10/2022 10:32:25 am ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[occurrences](
	[occurrenceId] [nvarchar](50) NOT NULL,
	[status] [varchar](16) NOT NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[sourceJobId] [int] IDENTITY(1,1) NOT NULL,
 CONSTRAINT [PK_occurrences] PRIMARY KEY CLUSTERED 
(
	[occurrenceId] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


